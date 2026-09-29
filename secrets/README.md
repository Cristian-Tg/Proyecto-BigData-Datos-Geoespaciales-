# Carpeta de secretos

**Esta carpeta está vacía a propósito y debe seguir estándolo en el repositorio.**

`.gitignore` ignora todo su contenido salvo este README y `.gitkeep`, así que
cualquier archivo que ponga aquí queda fuera del control de versiones.

## Token de Kaggle

Coloque aquí su `kaggle.json` para el uso local:

```
secrets/kaggle.json
```

Se obtiene en <https://www.kaggle.com/settings/account> → **Create New Token**.

El contenedor `ingestion` lo lee montado como `/run/secrets/kaggle.json` cuando
se usa la sobrecarga de desarrollo:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml run --rm ingestion
```

Con el compose principal, las credenciales viajan por las variables
`KAGGLE_USERNAME` y `KAGGLE_KEY` (desde `.env` en local, desde el almacén de
credenciales en Jenkins).

## En Jenkins

El token **no** se copia aquí. Se guarda como credencial:

**Manage Jenkins → Credentials → System → Global credentials → Add Credentials**

| Tipo | ID |
|---|---|
| Secret file | `kaggle-json` |
| Secret text | `mongo-root-password` |

## Comprobación

Antes de hacer `git push`, confirme que no se cuela nada:

```bash
git status --ignored secrets/
git ls-files secrets/          # solo debe listar .gitkeep y README.md
```

El pipeline de Jenkins aborta el build si detecta `kaggle.json` o `.env`
versionados.
