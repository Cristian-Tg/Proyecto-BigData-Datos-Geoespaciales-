/* ==========================================================================
 * Inicializacion del esquema geoespacial.
 *
 * Este script lo ejecuta el entrypoint de MongoDB UNA SOLA VEZ, en el primer
 * arranque con el directorio de datos vacio. Deja la base lista para recibir
 * la ingesta de Dask: colecciones creadas, validador de GeoJSON activo e
 * indice 2dsphere en su sitio.
 *
 * Es idempotente, para poder reejecutarlo a mano con
 *   docker compose exec mongo mongosh /docker-entrypoint-initdb.d/01-init-geo.js
 * sin romper nada.
 * ========================================================================== */

/* Se usa `geodb` y no `db` como nombre de variable: declarar `const db` cuando
 * el lado derecho tambien usa `db` provoca un ReferenceError por la zona
 * muerta temporal de JavaScript. */
var DB_NAME = (typeof process !== "undefined" && process.env && process.env.MONGO_DB)
  ? process.env.MONGO_DB : "geobigdata";
var MAIN = (typeof process !== "undefined" && process.env && process.env.MONGO_COLLECTION)
  ? process.env.MONGO_COLLECTION : "accidents";

var geodb = db.getSiblingDB(DB_NAME);

print("");
print("======================================================================");
print("  Inicializando la base geoespacial: " + DB_NAME);
print("======================================================================");

/* --------------------------------------------------------------------------
 * Coleccion principal con validador de esquema.
 *
 * El validador es la ultima linea de defensa: aunque la limpieza de Dask
 * fallara, MongoDB rechaza cualquier documento cuyo `location` no sea un
 * GeoJSON Point valido con longitud en [-180,180] y latitud en [-90,90].
 *
 * validationLevel "moderate": los documentos nuevos deben cumplir el esquema,
 * pero no se revalidan los que ya estaban, lo que evita penalizar la carga.
 * ------------------------------------------------------------------------ */
var geoValidator = {
  $jsonSchema: {
    bsonType: "object",
    required: ["location"],
    properties: {
      location: {
        bsonType: "object",
        required: ["type", "coordinates"],
        description: "GeoJSON Point: {type:'Point', coordinates:[lon, lat]}",
        properties: {
          type: { enum: ["Point"] },
          coordinates: {
            bsonType: "array",
            minItems: 2,
            maxItems: 2,
            /* Validacion posicional: [0] es longitud, [1] es latitud.
             * Este es el error clasico de GeoJSON y el validador lo atrapa. */
            items: [
              { bsonType: "number", minimum: -180, maximum: 180 },
              { bsonType: "number", minimum: -90,  maximum: 90  }
            ]
          }
        }
      },
      severity: { bsonType: ["int", "long", "double", "null"],
                  minimum: 1, maximum: 4 }
    }
  }
};

var existing = geodb.getCollectionNames();

if (existing.indexOf(MAIN) === -1) {
  geodb.createCollection(MAIN, {
    validator: geoValidator,
    validationLevel: "moderate",
    validationAction: "error"
  });
  print("  [+] Coleccion creada con validador GeoJSON: " + MAIN);
} else {
  geodb.runCommand({
    collMod: MAIN,
    validator: geoValidator,
    validationLevel: "moderate",
    validationAction: "error"
  });
  print("  [=] Validador actualizado en la coleccion existente: " + MAIN);
}

/* --------------------------------------------------------------------------
 * Indice 2dsphere: requisito explicito del enunciado.
 * Sin el, $near y $geoNear devuelven error y $geoWithin degrada a escaneo
 * completo de la coleccion.
 * ------------------------------------------------------------------------ */
geodb[MAIN].createIndex({ location: "2dsphere" }, { name: "ix_location_2dsphere" });
print("  [+] Indice 2dsphere sobre 'location'");

/* Indices de apoyo para los filtros que exponen los endpoints */
geodb[MAIN].createIndex({ start_time: -1 },        { name: "ix_start_time" });
geodb[MAIN].createIndex({ severity: 1 },           { name: "ix_severity" });
geodb[MAIN].createIndex({ state: 1, severity: 1 }, { name: "ix_state_severity" });
geodb[MAIN].createIndex({ year: 1, month: 1 },     { name: "ix_year_month" });
geodb[MAIN].createIndex({ grid_id: 1 },            { name: "ix_grid_id" });
geodb[MAIN].createIndex({ geohash: 1 },            { name: "ix_geohash" });
/* Clave natural: hace la ingesta idempotente frente a reejecuciones */
geodb[MAIN].createIndex({ accident_id: 1 },
                        { name: "ux_accident_id", unique: true, sparse: true });
print("  [+] Indices de apoyo y clave unica accident_id");

/* --------------------------------------------------------------------------
 * Colecciones que produce Spark. Se crean vacias con su indice 2dsphere para
 * que la API pueda consultarlas sin error antes del primer procesamiento.
 * ------------------------------------------------------------------------ */
var derived = [
  ["agg_grid",       "centroid"],
  ["agg_geohash",    "centroid"],
  ["agg_hotspots",   "centroid"],
  ["agg_state",      "centroid"],
  ["agg_temporal",   null],
  ["benchmark_runs", null]
];

derived.forEach(function (pair) {
  var name = pair[0], geoField = pair[1];
  if (geodb.getCollectionNames().indexOf(name) === -1) {
    geodb.createCollection(name);
  }
  if (geoField) {
    var spec = {};
    spec[geoField] = "2dsphere";
    geodb[name].createIndex(spec, { name: "ix_" + geoField + "_2dsphere" });
  }
  print("  [+] Coleccion derivada lista: " + name +
        (geoField ? " (2dsphere en " + geoField + ")" : ""));
});

print("======================================================================");
print("  Base geoespacial inicializada correctamente");
print("======================================================================");
print("");
