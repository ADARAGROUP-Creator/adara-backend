# ADARA — Zonificación MEF (Flex)

Última actualización: 23 Septiembre 2026 (doc **nuevo**: se separa de `ADARA-FLEX.md` el diccionario de zonificación de **Logística de Envíos MEF** — 26 zonas, localidades por partido, split La Matanza Norte/Sur, normalizaciones de texto ZPL y excepciones comerciales. `ADARA-FLEX.md` queda con arquitectura, sync y pantalla; acá vive la tabla de referencia.)

> 📌 **Qué es y qué no es este doc.**
> **Es** la referencia de cómo **MEF** zonifica el AMBA para facturar los envíos Flex.
> **No es** el criterio de clasificación de la app. El partido de un envío sale del **dato de la venta** (`lat/long` → Georef), decisión **F7**. Ver la cascada en `ADARA-FLEX.md`.
> Este diccionario se usa para: (1) el split **La Matanza Norte/Sur**, que ningún dato geográfico resuelve; (2) **control** de etiquetas contra el resumen semanal de MEF; (3) **excepciones** comerciales donde MEF zonifica distinto a la geografía.

---

## Las 26 zonas de MEF

Son **partidos** (más CABA), no la grilla tarifaria. Mapean a la grilla `CABA / GBA 1 / GBA 2 / GBA 3` vía `flex_partido_zona`.

Orden canónico del cuadro de control semanal:

| # | Zona |
|---|------|
| 1 | C.A.B.A |
| 2 | MATANZA NORTE |
| 3 | SAN MARTIN |
| 4 | 3 DE FEBRERO |
| 5 | MORON |
| 6 | MERLO |
| 7 | MATANZA SUR |
| 8 | LOMAS DE ZAMORA |
| 9 | BERAZATEGUI |
| 10 | FLORENCIO VARELA |
| 11 | QUILMES |
| 12 | ALTE BROWN |
| 13 | AVELLANEDA |
| 14 | EZEIZA |
| 15 | ESTEBAN ECHEVERRIA |
| 16 | LANUS |
| 17 | MORENO |
| 18 | ITUZAINGO |
| 19 | HURLINGHAM |
| 20 | JOSE C PAZ |
| 21 | SAN MIGUEL |
| 22 | MALVINAS ARGENTINAS |
| 23 | VICENTE LOPEZ |
| 24 | SAN ISIDRO |
| 25 | SAN FERNANDO |
| 26 | TIGRE |

---

## Diccionario localidad → zona

> ⚠️ Las localidades marcadas **(AMBIGUA)** pertenecen a más de un partido: **nunca** se resuelven por nombre, siempre por `lat/long`. En `flex_localidad_partido` van con `ambigua = true`.

### C.A.B.A
Los 48 barrios oficiales: Agronomía, Almagro, Balvanera, Barracas, Belgrano, Boedo, Caballito, Chacarita, Coghlan, Colegiales, Constitución, Flores, Floresta, La Boca, La Paternal, Liniers, Mataderos, Monte Castro, Monserrat, Nueva Pompeya, Núñez, Palermo, Parque Avellaneda, Parque Chacabuco, Parque Chas, Parque Patricios, Puerto Madero, Recoleta, Retiro, Saavedra, San Cristóbal, San Nicolás, San Telmo, Vélez Sarsfield, Versalles, Villa Crespo, Villa del Parque, Villa Devoto, Villa General Mitre, Villa Lugano, Villa Luro, Villa Ortúzar, Villa Pueyrredón, Villa Real, Villa Riachuelo, Villa Santa Rita, Villa Soldati, Villa Urquiza.

También aparecen denominaciones informales: Microcentro, Once, Abasto, Congreso, Las Cañitas, Palermo Soho, Palermo Hollywood. Todas son C.A.B.A.

### Zona Norte

| Zona | Localidades |
|------|-------------|
| **VICENTE LOPEZ** | Vicente López · Olivos · Florida · Florida Oeste · La Lucila · Munro · Carapachay · Villa Martelli · *Villa Adelina* **(AMBIGUA)** |
| **SAN ISIDRO** | San Isidro · Acassuso · Martínez · Beccar · Boulogne Sur Mer · *Villa Adelina* **(AMBIGUA)** |
| **SAN FERNANDO** | San Fernando · Victoria · Virreyes · islas/delta del partido |
| **TIGRE** | Tigre · Benavídez · General Pacheco · El Talar · Don Torcuato · Rincón de Milberg · Troncos del Talar · Ricardo Rojas · Dique Luján · *Nordelta* **(AMBIGUA: es urbanización, no partido)** |
| **SAN MARTIN** | General San Martín · San Martín · Villa Ballester · San Andrés · Villa Maipú · Billinghurst · José León Suárez · Villa Lynch · Villa Chacabuco |
| **3 DE FEBRERO** | Caseros · Ciudadela · Santos Lugares · Sáenz Peña · Villa Bosch · Martín Coronado · Pablo Podestá · Villa Raffo · José Ingenieros · Churruca · Once de Septiembre · El Libertador · *El Palomar* **(AMBIGUA)** |
| **SAN MIGUEL** | San Miguel · Muñiz · Bella Vista |
| **JOSE C PAZ** | José C. Paz · José Clemente Paz |
| **MALVINAS ARGENTINAS** | Los Polvorines · Grand Bourg · Pablo Nogués · Villa de Mayo · Tierras Altas · Ingeniero Adolfo Sourdeaux · *Tortuguitas* **(AMBIGUA)** |

### Zona Oeste

| Zona | Localidades |
|------|-------------|
| **HURLINGHAM** | Hurlingham · Villa Tesei · William C. Morris / William Morris |
| **ITUZAINGO** | Ituzaingó · Villa Udaondo |
| **MORON** | Morón · Castelar · Haedo · *El Palomar* **(AMBIGUA)** |
| **MERLO** | Merlo · San Antonio de Padua · Libertad · Mariano Acosta · Parque San Martín · Pontevedra |
| **MORENO** | Moreno · Paso del Rey · La Reja · Francisco Álvarez · Trujui · Cuartel V |

### Zona Sur

| Zona | Localidades |
|------|-------------|
| **AVELLANEDA** | Avellaneda · Wilde · Sarandí · Villa Domínico · Dock Sud · Piñeyro · *Gerli* **(AMBIGUA)** |
| **LANUS** | Lanús · Lanús Oeste · Lanús Este · Remedios de Escalada · Valentín Alsina · Monte Chingolo · *Gerli* **(AMBIGUA)** |
| **LOMAS DE ZAMORA** | Lomas de Zamora · Banfield · Temperley · Llavallol · Turdera · Villa Fiorito · Ingeniero Budge · Villa Centenario |
| **ALTE BROWN** | Adrogué · Burzaco · Longchamps · Glew · Claypole · Rafael Calzada · José Mármol · San José · Ministro Rivadavia · **Malvinas Argentinas (la localidad, no el partido del norte)** |
| **ESTEBAN ECHEVERRIA** | Monte Grande · Luis Guillón · El Jagüel · 9 de Abril · *Canning* **(AMBIGUA)** |
| **EZEIZA** | Ezeiza · Tristán Suárez · Carlos Spegazzini · La Unión · *Canning* **(AMBIGUA)** |
| **QUILMES** | Quilmes · Quilmes Oeste · Bernal · Bernal Oeste · Don Bosco · Ezpeleta · Ezpeleta Oeste · *San Francisco Solano* **(AMBIGUA)** |
| **BERAZATEGUI** | Berazategui · Berazategui Oeste · Hudson · Guillermo Enrique Hudson · Ranelagh · Plátanos · Villa España · Sourigues · Pereyra · El Pato · Juan María Gutiérrez |
| **FLORENCIO VARELA** | Florencio Varela · Bosques · Zeballos · Villa Vatteone · Gobernador Costa · Ingeniero Allan · La Capilla · Villa San Luis · Santa Rosa · Villa Brown |

> ⚠️ **Malvinas Argentinas** aparece dos veces: es un **partido** del GBA norte y una **localidad** de Almirante Brown en el sur. Ante ambigüedad, mirar partido y CP.

---

## La Matanza: el split Norte / Sur

Es **sub-zona comercial de MEF**, no un partido oficial. Georef devuelve "La Matanza" a secas, así que **el split sale de la localidad/CP**, no del dato geográfico.

| Sub-zona | Grilla | Localidades |
|----------|--------|-------------|
| **MATANZA NORTE** | GBA 1 | Lomas del Mirador · San Justo · Villa Luzuriaga · Villa Celina · Ramos Mejía · Ciudad Madero · Villa Madero · **La Tablada** |
| **MATANZA SUR** | GBA 2 | Isidro Casanova · Gregorio de Laferrere · Rafael Castillo · González Catán · Ciudad Evita · Virrey del Pino · Aldo Bonzi |

Otras localidades del partido que pueden aparecer sin Norte/Sur definido: Tapiales, 20 de Junio. **No inventar la división**: si no está en la lista ni en la etiqueta, va a REVISAR.

**La Tablada** quedó en **NORTE (GBA 1)** el 23/9/2026: las etiquetas de MEF la clasifican así y la etiqueta es el criterio de quien factura. La auditoría previa que sugería GBA 2 queda descartada.

---

## Etiquetas ZPL (control, no clasificación)

Las etiquetas que imprime ML para MEF traen la zona escrita por el propio operador logístico:

```
^FO0,660 ... ^FD[ZONA]^FS        ← zona MEF: MANDA
^FO0,705 ... ^FD[LOCALIDAD]^FS   ← localidad/barrio: NO es una segunda zona
```

Ejemplo: `^FO0,660…^FDTIGRE^FS` + `^FO0,705…^FDBENAVIDEZ^FS` = **1 envío en TIGRE**, no dos, y no Benavídez.

Reglas de lectura:
1. Si existe `^FO0,660`, **ese campo determina la zona** y no se corrige por el barrio de `^FO0,705`.
2. Si falta o es ilegible, resolver por dirección + CP.
3. Si la localidad puede ser de más de un partido, **no adivinar**: verificar dirección completa y CP.
4. Si no se puede determinar, marcar **REVISAR** y mostrar la dirección.

**Normalizaciones de texto** (los TXT/ZPL vienen con la codificación escapada):

| En el archivo | Normalizado |
|---------------|-------------|
| `CABA` | C.A.B.A |
| `LA MATANZA NORTE` | MATANZA NORTE |
| `LA MATANZA SUR` | MATANZA SUR |
| `TRES DE FEBRERO` | 3 DE FEBRERO |
| `SAN MART_C3_8DN` | SAN MARTIN |
| `MOR_C3_93N` | MORON |
| `LAN_C3_9AS` | LANUS |
| `ESTEBAN ECHEVERR_C3_8DA` | ESTEBAN ECHEVERRIA |
| `ITUZAING_C3_93` | ITUZAINGO |
| `JOS_C3_89 C_2E PAZ` | JOSE C PAZ |
| `JOSE CLEMENTE PAZ` | JOSE C PAZ |
| `VICENTE L_C3_93PEZ` | VICENTE LOPEZ |
| `ALMIRANTE BROWN` | ALTE BROWN |

Al contar etiquetas: cada etiqueta cuenta **una** vez, se devuelven las 26 zonas aunque tengan cero, y la suma tiene que dar exacto el total de etiquetas del archivo.

---

## Excepciones comerciales (MEF zonifica distinto a la geografía)

Casos donde el dato geográfico es correcto pero MEF factura otra zona. Se cargan a mano cuando aparecen en el control semanal.

| Caso | Geografía | MEF cobra | Estado |
|------|-----------|-----------|--------|
| 2 envíos de barrio limítrofe (semana 16/02–21/02) | CABA | GBA 1 | Detectado en auditoría, diferencia $2.600. Sin identificar cuáles |
| La Tablada | La Matanza | MATANZA NORTE (GBA 1) | **Resuelto 23/9/2026** |

**A confirmar con MEF:** localidades donde la grilla comercial pueda no coincidir con el partido oficial. Se agregan acá a medida que aparezcan diferencias en el control semanal contra el resumen.

---

## Relación con el resto de la documentación

| Tema | Doc |
|------|-----|
| Arquitectura Flex, tablas, vistas, pantalla, cascada de resolución de zona | `ADARA-FLEX.md` |
| Regla F7 (el partido sale del dato, no del nombre) | `ADARA-DECISIONES.md` |
| Precios por zona y grilla tarifaria | `ADARA-FLEX.md` §Zonas y precios |
