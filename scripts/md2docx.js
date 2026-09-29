/**
 * Convierte el informe tecnico de Markdown a .docx.
 *
 * El objetivo de formato es que quepa en 10 paginas (limite del enunciado), asi
 * que los tamanos son compactos a proposito y estan agrupados en CONFIG para
 * poder ajustarlos de un sitio.
 *
 * Uso:  node md2docx.js <entrada.md> <salida.docx> [--body 19] [--margin 1008]
 */
const fs = require('fs');
const path = require('path');
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType,
  Table, TableRow, TableCell, WidthType, BorderStyle, ShadingType,
  LevelFormat, PageOrientation, ExternalHyperlink, convertInchesToTwip,
} = require('docx');

// ---------------------------------------------------------------------------
// Configuracion de formato. Los tamanos van en MEDIOS puntos (docx los usa asi):
// 19 = 9,5 pt.  Bajar `body` es la palanca directa para reducir paginas.
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
const IN = args[0];
const OUT = args[1];
const argNum = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 ? parseInt(args[i + 1], 10) : def;
};

const CONFIG = {
  body: argNum('--body', 19),        // 9,5 pt
  margin: argNum('--margin', 1008),  // 0,7 pulgadas (1440 = 1")
  h1: 30,                            // 15 pt
  h2: 24,                            // 12 pt
  h3: 21,                            // 10,5 pt
  h4: 19,
  table: 17,                         // 8,5 pt
  code: 16,                          // 8 pt
  diagram: 12,                       // 6 pt — el diagrama es de ~72 columnas
  lineSpacing: 240,                  // 1,0 lineas (240 = simple)
  page: { width: 12240, height: 15840 },  // US Letter en DXA
};

const TEXT_WIDTH = CONFIG.page.width - 2 * CONFIG.margin;
const MONO = 'Consolas';
const SERIF = 'Calibri';

// ---------------------------------------------------------------------------
// Formato en linea: **negrita**, `codigo`, *cursiva*, [texto](url)
// ---------------------------------------------------------------------------
function inlineRuns(text, base = {}) {
  const runs = [];
  // Se procesa con una sola expresion alternativa para respetar el orden de
  // aparicion; anidar pasadas sucesivas romperia `**texto con `codigo`**`.
  const re = /(\*\*[^*]+\*\*)|(`[^`]+`)|(\[[^\]]+\]\([^)]+\))|(\*[^*\n]+\*)/g;
  let last = 0, m;

  const push = (t, extra) => {
    if (t) runs.push(new TextRun({ text: t, ...base, ...extra }));
  };

  while ((m = re.exec(text)) !== null) {
    push(text.slice(last, m.index));
    const tok = m[0];
    if (m[1]) {
      push(tok.slice(2, -2), { bold: true });
    } else if (m[2]) {
      push(tok.slice(1, -1), { font: MONO, size: CONFIG.code });
    } else if (m[3]) {
      const mm = /\[([^\]]+)\]\(([^)]+)\)/.exec(tok);
      const url = mm[2];
      if (/^https?:\/\//.test(url)) {
        runs.push(new ExternalHyperlink({
          children: [new TextRun({ text: mm[1], ...base, style: 'Hyperlink' })],
          link: url,
        }));
      } else {
        // Enlace a otro archivo del repositorio: se deja el texto, la ruta
        // entre parentesis no aporta nada en un documento impreso.
        push(mm[1], { italics: true });
      }
    } else if (m[4]) {
      push(tok.slice(1, -1), { italics: true });
    }
    last = m.index + tok.length;
  }
  push(text.slice(last));
  return runs.length ? runs : [new TextRun({ text: '', ...base })];
}

const para = (text, opts = {}) => new Paragraph({
  children: inlineRuns(text, { size: CONFIG.body, font: SERIF }),
  spacing: { line: CONFIG.lineSpacing, after: 80 },
  ...opts,
});

// ---------------------------------------------------------------------------
// Tablas
// ---------------------------------------------------------------------------
const PIPE = '\u0000PIPE\u0000';

function splitRow(line) {
  return line.replace(/\\\|/g, PIPE)
    .replace(/^\s*\|/, '').replace(/\|\s*$/, '')
    .split('|')
    .map(c => c.replace(new RegExp(PIPE, 'g'), '|').trim());
}

function buildTable(lines) {
  const header = splitRow(lines[0]);
  const sep = splitRow(lines[1]);
  const rows = lines.slice(2).map(splitRow);
  const n = header.length;

  // Alineacion desde la fila separadora: ---: derecha, :---: centro
  const align = sep.map(s => {
    if (/^:?-+:$/.test(s)) return AlignmentType.CENTER;
    if (/-+:$/.test(s)) return AlignmentType.RIGHT;
    return AlignmentType.LEFT;
  });

  // Anchos proporcionales al contenido mas largo de cada columna, acotados para
  // que ninguna columna se quede en un hilo ni se coma el resto.
  const raw = [];
  for (let c = 0; c < n; c++) {
    let max = header[c] ? header[c].length : 1;
    for (const r of rows) {
      const len = (r[c] || '').replace(/[*`]/g, '').length;
      if (len > max) max = len;
    }
    raw.push(Math.min(Math.max(max, 4), 60));
  }
  const total = raw.reduce((a, b) => a + b, 0);
  const widths = raw.map(w => Math.max(700, Math.round(TEXT_WIDTH * w / total)));
  // Cuadrar la suma exacta: docx exige que columnWidths sume el ancho de tabla
  const diff = TEXT_WIDTH - widths.reduce((a, b) => a + b, 0);
  widths[widths.length - 1] += diff;

  const thinBorder = {
    style: BorderStyle.SINGLE, size: 2, color: 'BFBFBF',
  };
  const borders = {
    top: thinBorder, bottom: thinBorder, left: thinBorder,
    right: thinBorder, insideHorizontal: thinBorder, insideVertical: thinBorder,
  };

  const cell = (txt, c, isHeader) => new TableCell({
    width: { size: widths[c], type: WidthType.DXA },
    shading: isHeader
      ? { type: ShadingType.CLEAR, fill: 'EDEDED', color: 'auto' }
      : undefined,
    margins: { top: 40, bottom: 40, left: 90, right: 90 },
    children: [new Paragraph({
      alignment: align[c],
      spacing: { line: 220, after: 0 },
      children: inlineRuns(txt || '', {
        size: CONFIG.table, font: SERIF, bold: isHeader || undefined,
      }),
    })],
  });

  return new Table({
    columnWidths: widths,
    width: { size: TEXT_WIDTH, type: WidthType.DXA },
    borders,
    rows: [
      new TableRow({
        tableHeader: true,
        children: header.map((h, c) => cell(h, c, true)),
      }),
      ...rows.map(r => new TableRow({
        children: Array.from({ length: n }, (_, c) => cell(r[c], c, false)),
      })),
    ],
  });
}

// ---------------------------------------------------------------------------
// Bloques de codigo
// ---------------------------------------------------------------------------
function codeParagraphs(lines, isDiagram) {
  const size = isDiagram ? CONFIG.diagram : CONFIG.code;
  return lines.map((l, i) => new Paragraph({
    children: [new TextRun({ text: l || ' ', font: MONO, size })],
    spacing: { line: isDiagram ? 180 : 200, after: 0, before: i === 0 ? 60 : 0 },
    shading: { type: ShadingType.CLEAR, fill: 'F7F7F7', color: 'auto' },
    indent: { left: 120 },
  }));
}

// ---------------------------------------------------------------------------
// Parseo del Markdown
// ---------------------------------------------------------------------------
const md = fs.readFileSync(IN, 'utf8').split(/\r?\n/);
const children = [];
let i = 0;

while (i < md.length) {
  const line = md[i];

  // --- bloque de codigo ---------------------------------------------------
  if (/^\s*```/.test(line)) {
    const buf = [];
    i++;
    while (i < md.length && !/^\s*```/.test(md[i])) { buf.push(md[i]); i++; }
    i++;
    // El diagrama de arquitectura se detecta por sus caracteres de caja
    const isDiagram = buf.some(l => /[┌└│─▼►◀]/.test(l));
    children.push(...codeParagraphs(buf, isDiagram));
    children.push(new Paragraph({ text: '', spacing: { after: 80 } }));
    continue;
  }

  // --- tabla --------------------------------------------------------------
  if (/^\s*\|/.test(line) && i + 1 < md.length && /^\s*\|[\s:|-]+\|?\s*$/.test(md[i + 1])) {
    const buf = [];
    while (i < md.length && /^\s*\|/.test(md[i])) { buf.push(md[i]); i++; }
    children.push(buildTable(buf));
    children.push(new Paragraph({ text: '', spacing: { after: 100 } }));
    continue;
  }

  // --- encabezados --------------------------------------------------------
  let m;
  if ((m = /^#\s+(.*)$/.exec(line))) {
    children.push(new Paragraph({
      children: inlineRuns(m[1], { size: CONFIG.h1, bold: true, font: SERIF }),
      heading: HeadingLevel.TITLE,
      spacing: { before: 0, after: 60 },
    }));
    i++; continue;
  }
  if ((m = /^##\s+(.*)$/.exec(line))) {
    const txt = m[1];
    // Los "## N." son secciones de primer nivel; el subtitulo del principio no
    const isSection = /^\d+\.|^Anexo/.test(txt);
    children.push(new Paragraph({
      children: inlineRuns(txt, {
        size: isSection ? CONFIG.h2 : CONFIG.h3,
        bold: true, font: SERIF,
        color: isSection ? '1F4E79' : '404040',
      }),
      heading: isSection ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
      spacing: { before: isSection ? 220 : 120, after: 80 },
    }));
    i++; continue;
  }
  if ((m = /^###\s+(.*)$/.exec(line))) {
    children.push(new Paragraph({
      children: inlineRuns(m[1], { size: CONFIG.h3, bold: true, font: SERIF,
                                   color: '2E5F8A' }),
      heading: HeadingLevel.HEADING_2,
      spacing: { before: 160, after: 60 },
    }));
    i++; continue;
  }
  if ((m = /^####\s+(.*)$/.exec(line))) {
    children.push(new Paragraph({
      children: inlineRuns(m[1], { size: CONFIG.h4, bold: true, italics: true,
                                   font: SERIF }),
      heading: HeadingLevel.HEADING_3,
      spacing: { before: 130, after: 50 },
    }));
    i++; continue;
  }

  // --- regla horizontal ---------------------------------------------------
  if (/^\s*---+\s*$/.test(line)) {
    children.push(new Paragraph({
      text: '',
      spacing: { before: 60, after: 120 },
      border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: 'C0C0C0' } },
    }));
    i++; continue;
  }

  // --- cita ---------------------------------------------------------------
  if (/^>\s?/.test(line)) {
    const buf = [];
    while (i < md.length && /^>/.test(md[i])) {
      buf.push(md[i].replace(/^>\s?/, '')); i++;
    }
    // Las lineas en blanco separan parrafos dentro de la cita
    const bloques = buf.join('\n').split(/\n\s*\n/);
    for (const b of bloques) {
      if (!b.trim()) continue;
      children.push(new Paragraph({
        children: inlineRuns(b.replace(/\n/g, ' ').trim(),
                             { size: CONFIG.body - 1, font: SERIF, italics: true,
                               color: '505050' }),
        spacing: { line: CONFIG.lineSpacing, after: 70 },
        indent: { left: 240 },
        border: { left: { style: BorderStyle.SINGLE, size: 12, color: '9CC3E5',
                          space: 8 } },
      }));
    }
    continue;
  }

  // --- listas -------------------------------------------------------------
  if ((m = /^(\s*)([-*])\s+(.*)$/.exec(line))) {
    const nivel = Math.floor(m[1].length / 2);
    children.push(new Paragraph({
      children: inlineRuns(m[3], { size: CONFIG.body, font: SERIF }),
      numbering: { reference: 'vinetas', level: Math.min(nivel, 2) },
      spacing: { line: CONFIG.lineSpacing, after: 40 },
    }));
    i++; continue;
  }
  if ((m = /^(\s*)(\d+)\.\s+(.*)$/.exec(line))) {
    const nivel = Math.floor(m[1].length / 3);
    children.push(new Paragraph({
      children: inlineRuns(m[3], { size: CONFIG.body, font: SERIF }),
      numbering: { reference: 'numerada', level: Math.min(nivel, 2) },
      spacing: { line: CONFIG.lineSpacing, after: 40 },
    }));
    i++; continue;
  }

  // --- linea en blanco ----------------------------------------------------
  if (!line.trim()) { i++; continue; }

  // --- parrafo: se juntan las lineas hasta la siguiente en blanco ---------
  const buf = [line];
  i++;
  while (i < md.length && md[i].trim() && !/^(#{1,4}\s|\s*\||\s*```|>|\s*[-*]\s|\s*\d+\.\s|\s*---+\s*$)/.test(md[i])) {
    buf.push(md[i]); i++;
  }
  children.push(para(buf.join(' ').replace(/\s+/g, ' ').trim()));
}

// ---------------------------------------------------------------------------
// Documento
// ---------------------------------------------------------------------------
const doc = new Document({
  creator: 'Equipo Big Data',
  title: 'Informe tecnico - Datos geoespaciales con despliegue continuo',
  description: 'Big Data: procesamiento y consulta de datos geoespaciales',
  styles: {
    default: {
      document: { run: { font: SERIF, size: CONFIG.body } },
    },
  },
  numbering: {
    config: [
      {
        reference: 'vinetas',
        levels: [0, 1, 2].map(l => ({
          level: l,
          format: LevelFormat.BULLET,
          text: ['•', '◦', '▪'][l],
          alignment: AlignmentType.LEFT,
          style: {
            paragraph: { indent: { left: 260 + l * 220, hanging: 200 } },
            run: { size: CONFIG.body, font: SERIF },
          },
        })),
      },
      {
        reference: 'numerada',
        levels: [0, 1, 2].map(l => ({
          level: l,
          format: LevelFormat.DECIMAL,
          text: `%${l + 1}.`,
          alignment: AlignmentType.LEFT,
          style: {
            paragraph: { indent: { left: 300 + l * 240, hanging: 240 } },
            run: { size: CONFIG.body, font: SERIF },
          },
        })),
      },
    ],
  },
  sections: [{
    properties: {
      page: {
        size: {
          width: CONFIG.page.width,
          height: CONFIG.page.height,
          orientation: PageOrientation.PORTRAIT,
        },
        margin: {
          top: CONFIG.margin, right: CONFIG.margin,
          bottom: CONFIG.margin, left: CONFIG.margin,
        },
      },
    },
    footers: {
      default: new (require('docx').Footer)({
        children: [new Paragraph({
          alignment: AlignmentType.CENTER,
          children: [
            new TextRun({ text: 'Informe tecnico - Big Data geoespacial   |   ',
                          size: 15, color: '808080', font: SERIF }),
            new TextRun({ children: [require('docx').PageNumber.CURRENT],
                          size: 15, color: '808080', font: SERIF }),
            new TextRun({ text: ' / ', size: 15, color: '808080', font: SERIF }),
            new TextRun({ children: [require('docx').PageNumber.TOTAL_PAGES],
                          size: 15, color: '808080', font: SERIF }),
          ],
        })],
      }),
    },
    children,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(OUT, buf);
  const kb = (buf.length / 1024).toFixed(0);
  console.log(`Escrito ${OUT} (${kb} KB)`);
  console.log(`  elementos: ${children.length}`);
  console.log(`  cuerpo ${CONFIG.body / 2} pt | margenes ${(CONFIG.margin / 1440).toFixed(2)}" | tablas ${CONFIG.table / 2} pt | diagrama ${CONFIG.diagram / 2} pt`);
});
