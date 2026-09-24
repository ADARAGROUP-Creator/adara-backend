# ADARA — Mantenimiento de la Documentación (protocolo)

Última actualización: 13 Julio 2026 (creación del protocolo tras la limpieza de duplicados).

Este documento define **cómo se mantiene viva la documentación de ADARA** entre conversaciones, para que nada se pierda al migrar de un chat a otro y para que no se vuelvan a generar documentos duplicados.

> 📌 **Regla base:** los `.md` de ADARA **no son archivos de la PC ni del repo de código**. Viven dentro del Proyecto de Claude "ADARA APP". Se leen y se editan **en el lugar**. No se suben ni se borran a mano.

---

## 1. Al iniciar cualquier conversación (onboarding)

1. Leer **`ADARA-DOCS-INDEX.md`** primero (es el punto de entrada).
2. Identificar el/los documento(s) del dominio del problema y leerlos antes de proponer cambios.
3. Para reglas de negocio, confirmar contra **`ADARA-DECISIONES.md`** (la "constitución").

No hace falta leer los 33 docs: se lee el índice + el/los del dominio puntual.

---

## 2. Durante la conversación (no perder nada)

Todo lo que se decide o descubre y tenga valor para el futuro **debe bajar a un `.md`**, no quedar solo en el chat. Esto incluye:

- una **decisión técnica o de negocio** nueva o modificada,
- una **regla** nueva / derogada / excepción,
- un **bug** resuelto (causa + fix),
- un **pendiente** nuevo,
- una **auditoría** o hallazgo relevante.

El texto que queda solo en el chat **no viaja** a otras conversaciones. Solo persiste lo que se escribe dentro de un doc.

---

## 3. Antes de cerrar / migrar a otro chat (handoff)

Cuando la conversación se está saturando (respuestas más lentas o imprecisas, historial muy largo) o antes de arrancar un chat nuevo:

1. Repasar qué produjo la charla (decisiones, reglas, bugs, pendientes).
2. **Actualizar los `.md` que corresponda** con ese contenido, en el lugar.
3. Si cambió una regla → actualizar primero `ADARA-DECISIONES.md`, después propagar al doc temático.
4. Si nació un tema con entidad propia → crear un `.md` nuevo y **agregarlo al índice** (con su conteo).
5. Confirmar que el índice quede consistente (conteo, referencias, numeración).

Recién ahí se abre el chat nuevo. El chat nuevo arranca leyendo el índice (paso 1) y encuentra todo al día.

---

## 4. Regla de oro anti-duplicados

Los duplicados del proyecto (resueltos el 13/7/2026: INDEX, DECISIONES, FLEX, VENTAS-ML) se originaron, muy probablemente, por **re-subir archivos `.md` con el mismo nombre desde la interfaz del proyecto** (subir un nombre repetido no siempre pisa al anterior: puede dejar dos copias).

**Para que no vuelva a pasar:**
- **NO** re-subir un `.md` con nombre existente desde la UI del proyecto.
- Para modificar un doc, pedirle a Claude que lo **edite en el lugar** (reemplaza el contenido, no crea copias).
- Editar un doc = leerlo, cambiarlo y volver a escribirlo en la **misma ruta**.

---

## 5. Concurrencia

Un `.md` lo debe tocar **un solo chat a la vez**. Si dos conversaciones editan el mismo doc en paralelo, el último en guardar pisa al otro (last-write-wins). En la práctica no es problema trabajando de a un chat por vez.

---

## 6. Checklist de cierre de sesión

- [ ] ¿Hubo decisiones/reglas/bugs/pendientes nuevos? → bajados a su `.md`.
- [ ] ¿Cambió una regla? → `ADARA-DECISIONES.md` actualizado + doc temático propagado.
- [ ] ¿Se creó un doc nuevo? → agregado a `ADARA-DOCS-INDEX.md` + conteo actualizado.
- [ ] ¿El índice quedó consistente (referencias, numeración, conteo)?
- [ ] ¿Nada quedó "solo en el chat"?

---

## 7. Respaldo opcional (fuera del proyecto)

Si en algún momento se quiere historial de versiones de la documentación (quién cambió qué y cuándo), se pueden exportar los `.md` a un repo de GitHub aparte del código. Es **opcional** y no hace falta para la operación diaria: el proyecto ya conserva la versión vigente de cada doc.
