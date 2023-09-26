const { QueryTypes, and } = require('sequelize');
const {sequelize} = require('../BD-RFAS8/conexiondb');
const ExcelJS = require('exceljs');

exports.cursoVida = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO; 
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const page = req.query.page || 1;
        const limit = req.query.limit || 50;
        const offset = (page - 1) * limit;
        const query = `;WITH NumberedResults AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY a.NUMDOCUM ASC) AS RowNum,
                f.IPS,
                g.NOMBRE AS NOMBRE_IPS,
                    CASE a.TIPDOCUM 
                    WHEN 1 THEN 'CEDULA DE CIUDADANIA'
                    WHEN 2 THEN 'TARJETA DE IDENTIDAD'
                    WHEN 3 THEN 'CEDULA DE EXTRANJERIA'
                    WHEN 4 THEN 'REGISTRO CIVIL'
                    WHEN 5 THEN 'PASAPORTE'
                    WHEN 6 THEN 'ADULTO SIN IDENTIFICACION'
                    WHEN 7 THEN 'MENOR SIN IDENTIFICACION'
                    WHEN 9 THEN 'NACIDO VIVO'
                    WHEN 10 THEN 'SALVO CONDUCTO'
                    WHEN 12 THEN 'CARNE DIPLOMATICO'
                    WHEN 13 THEN 'PERMISO ESPECIAL'
                    WHEN 14 THEN 'RECIDENTE ESPECIAL'
                    WHEN 15 THEN 'PERMISO PROTECCION TEMPORAL'
                    ELSE 'OTRO'
                    END AS TIPO_DOCUMENTO,
                    a.POBLACION_ESPECIAL, 
                    a.APELLIDO1 + ' ' + a.APELLIDO2 AS APELLIDOS,
                    a.NOMBRE1 + '  ' + a.NOMBRE2 AS NOMBRES,
                    DATEADD(DAY, 1, CAST(a.FECHANAC AS DATE)) AS FECHA_NACIMIENTO,  
                    CASE a.SEXO 
                    WHEN 2 THEN 'FEMENINO'
                    WHEN 1 THEN 'MASCULINO' 
                    ELSE 'OTRO'
                    END AS GENERO, 
                    CASE e.EMBARAZO 
                    WHEN 0 THEN 'NO'
                    WHEN 1 THEN 'EMBARAZADA' 
                    END AS EMBARAZO, 
                    CASE a.TIPDISCAP 
                    WHEN 1 THEN 'CONDUCTA'
                    WHEN 2 THEN 'COMUNICACION'
                    WHEN 3 THEN 'CUIDADO PERSONAL'
                    WHEN 4 THEN 'LOCOMOCION'
                    WHEN 5 THEN 'DISPOSICION DEL CUERPO'
                    WHEN 6 THEN 'DESTREZA'
                    WHEN 7 THEN 'SITUACION'
                    WHEN 8 THEN 'DETERMINADA ACTITUD'
                    WHEN 9 THEN 'OTRO'
                    END AS TIPO_DISCAPACIDAD, 
                    CASE a.GRDDISCAP 
                    WHEN 1 THEN 'LEVE'
                    WHEN 2 THEN 'MODERADO' 
                    WHEN 3 THEN 'SEVERA'
                    END AS GRADO_DISCAPACIDAD, 
                    a.DIRECRES AS DIRECCION_RESIDENCIA, 
                    a.TELEFRES AS TELEFONO_RESIDENCIA, 
                    a.CODBARES AS CODIGO_BARRIO, 
                    d.NOMBRE AS COMUNA, 
                    UPPER(c.NOMBRE) AS BARRIO, 
                    CASE a.ETNICO 
                    WHEN 1 THEN 'BLANCO'
                    WHEN 2 THEN 'INDIGENA'
                    WHEN 3 THEN 'AFRODECENDIENTE'
                    WHEN 4 THEN 'MESTIZO(IND-BLA)'
                    WHEN 5 THEN 'MULATO(NEG-BLA)'
                    WHEN 6 THEN 'ZAMBO(IND-NEG)'
                    WHEN 7 THEN 'GITANO(ROM)'
                    WHEN 8 THEN 'RAIZAL(SAN ANDRES)'
                    WHEN 9 THEN 'PALENQUERO'
                    ELSE 'OTRO'
                    END AS ETNICO, 
                    UPPER(b.NOMBRE) AS PAIS,  
                    h.NOMBRE AS CENTRO_PRODUCCION, 
                    f.CODIGO, 
                    CASE f.ATENDIDO 
                    WHEN 0 THEN 'NO'
                    WHEN 1 THEN 'SI'
                    END AS ATENDIDO, 
                    DATEADD(DAY, 1, CAST(e.FECHA AS DATE)) AS FECHA, 
                    DATEADD(DAY, 1, CAST(f.ATENCION_FACTURA AS DATE)) AS ATENCION_FACTURA, 
                    CASE f.ESTADO 
                    WHEN  0 THEN 'DISPONIBLE'
                    WHEN  1 THEN 'CONFIRMADO'
                    WHEN  2 THEN 'INCUMPLIDA'
                    WHEN  3 THEN 'CANCELADAS'
                    WHEN  4 THEN 'CANCELADA POR EL PRESTADOR'
                    END AS ESTADO, 
                    DATEPART(DAY, a.FECHANAC) AS DIA, 
                    DATEPART(MONTH, a.FECHANAC) AS MES, 
                    DATEPART(YEAR, a.FECHANAC) AS AÑO, 
                    CONCAT(e.CANTEDAD, + ' ' +
                    CASE e.FORMEDAD 
                    WHEN 1 THEN 'AÑOS'
                    WHEN 2 THEN 'MESES'
                    WHEN 3 THEN 'DIAS'
                    WHEN 4 THEN 'HORAS'
                    END) AS EDAD,  
                    a.NUMDOCUM AS NUMERO_DOCUMENTO,
                    f.CODIGO_CUPS AS DescripcionServicio
                    FROM  fac_m_tarjetero a 
                    JOIN gen_p_paises b ON b.PAIS = a.PAIS  
                    JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
                    JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
                    JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
                    JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
                    JOIN fac_p_control g ON g.IPS = f.IPS 
                    JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD  
                    JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS 
                    WHERE h.CODIGO = ${CODIGO}
                    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
                    AND f.ATENCION_FACTURA IS NOT NULL
					AND f.ATENDIDO = 1
					AND f.CODIGO_CUPS IS NOT NULL
					AND f.ESTADO = 1
                    AND YEAR(f.ATENCION_FACTURA) BETWEEN (YEAR(GETDATE()) - 5) AND YEAR(GETDATE())
                    
            )
            SELECT DISTINCT
            NOMBRE_IPS,
            TIPO_DOCUMENTO,
            NUMERO_DOCUMENTO,
            APELLIDOS,
            NOMBRES,
            FECHA_NACIMIENTO,
            GENERO,
            EMBARAZO,
            DIRECCION_RESIDENCIA,
            TELEFONO_RESIDENCIA,
            COMUNA,
            BARRIO,
            ETNICO,
            PAIS,
            CODIGO,
            FECHA,
            ATENCION_FACTURA,
            ATENDIDO,
            EDAD,
            DescripcionServicio
            FROM NumberedResults
            WHERE RowNum BETWEEN 1 AND 1000`;
        const resul = await sequelize.query(query, {type: QueryTypes.SELECT});
        
        const queryCount = `SELECT DISTINCT
        COUNT(*) data
        FROM fac_m_tarjetero a
        JOIN gen_p_paises b ON b.PAIS = a.PAIS 
        JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
        JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
        JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
        JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
        JOIN fac_p_control g ON g.IPS = f.IPS
        JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
        JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
        WHERE h.CODIGO = ${CODIGO} AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal} 
        AND f.ATENCION_FACTURA IS NOT NULL
		AND f.ATENDIDO = 1
		AND f.CODIGO_CUPS IS NOT NULL
		AND f.ESTADO = 1
		AND YEAR(f.ATENCION_FACTURA) BETWEEN (YEAR(GETDATE()) - 5) AND YEAR(GETDATE())`
        const count = await sequelize.query(queryCount, {type: QueryTypes.SELECT});
        
        
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            page,
            totalpage: Math.round(count[0].data / limit),
            itemperpage: limit,
            islast: Math.round(count[0].data / limit) == page,
            total: count[0].data,
            data: resul
        })
        
    } catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
    }
}

exports.buscar = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO; 
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const primeraInfancia = [[890105], [890114], [890115], [890116], [890201], [890203], [890205], [890206], [890263], [890283], [890301], [890303], 
        [890305], [890306], [890363], [890383], [902213], [950601], [990101], [990102], [990103], [990104], [990105], [990106], 
        [990107], [990108], [990109], [990110], [990111], [990112], [990201], [990203], [990204], [990206], [990207], [990208], 
        [990209], [990212], [990213], [990221], [990222], [990223], [990224], [993102], [993106], [993122], [993130], [993501], 
        [99350101], [993502], [993503], [993504], [993509], [99351003], [993512], [993522], [997102], [997106], [997310], 
        ['P0000002'], ['P0000004'], ['P0000006'], ['P0000009'], ['P0000013'], ['P0000014']]
        const page = req.query.page || 1;
        const limit = req.query.limit || 50;
        const offset = (page - 1) * limit;
        const arrayId = req.body;
        console.log(req.body);
        let it = ""
        for (let i = 0; i < arrayId.length; i++) {
            const id = arrayId[i];
            if(i == 0){
                it +=  `AND a.NUMDOCUM IN ('${id}')`
            }
            else{
                it +=  `OR a.NUMDOCUM IN ('${id}')`
            }
            
            
        }
        
    const query = `SELECT DISTINCT *
	FROM
	(
		SELECT 
        f.IPS, 
        g.NOMBRE AS NOMBRE_IPS, 
        CASE a.TIPDOCUM
        WHEN 1 THEN 'CEDULA DE CIUDADANIA'
        WHEN 2 THEN 'TARJETA DE IDENTIDAD'
        WHEN 3 THEN 'CEDULA DE EXTRANJERIA'
        WHEN 4 THEN 'REGISTRO CIVIL'
        WHEN 5 THEN 'PASAPORTE'
        WHEN 6 THEN 'ADULTO SIN IDENTIFICACION'
        WHEN 7 THEN 'MENOR SIN IDENTIFICACION'
        WHEN 9 THEN 'NACIDO VIVO'
        WHEN 10 THEN 'SALVO CONDUCTO'
        WHEN 12 THEN 'CARNE DIPLOMATICO'
        WHEN 13 THEN 'PERMISO ESPECIAL'
        WHEN 14 THEN 'RECIDENTE ESPECIAL'
        WHEN 15 THEN 'PERMISO PROTECCION TEMPORAL'
        ELSE 'OTRO'
        END AS TIPO_DOCUMENTO,				 
        a.POBLACION_ESPECIAL,
		a.APELLIDO1 + '  ' + a.APELLIDO2 AS APELLIDOS,
        a.NOMBRE1 + '  ' + a.NOMBRE2 AS NOMBRES,
        DATEADD(DAY, 1, CAST(a.FECHANAC AS DATE)) AS FECHA_NACIMIENTO, 
        CASE a.SEXO
        WHEN 2 THEN 'FEMENINO'
        WHEN 1 THEN 'MASCULINO' 
        ELSE 'OTRO'
        END AS GENERO,
        CASE e.EMBARAZO
        WHEN 0 THEN 'NO'
        WHEN 1 THEN 'EMBARAZADA' 
        END AS EMBARAZO,
        CASE a.TIPDISCAP
        WHEN 1 THEN 'CONDUCTA'
        WHEN 2 THEN 'COMUNICACION'
        WHEN 3 THEN 'CUIDADO PERSONAL'
        WHEN 4 THEN 'LOCOMOCION'
        WHEN 5 THEN 'DISPOSICION DEL CUERPO'
        WHEN 6 THEN 'DESTREZA'
        WHEN 7 THEN 'SITUACION'
        WHEN 8 THEN 'DETERMINADA ACTITUD'
        WHEN 9 THEN 'OTRO'
        END AS TIPO_DISCAPACIDAD,
        CASE a.GRDDISCAP
        WHEN 1 THEN 'LEVE'
        WHEN 2 THEN 'MODERADO' 
        WHEN 3 THEN 'SEVERA'
        END AS GRADO_DISCAPACIDAD,
        a.DIRECRES AS DIRECCION_RESIDENCIA, 
    a.TELEFRES AS TELEFONO_RESIDENCIA,
        a.CODBARES AS CODIGO_BARRIO,
        d.NOMBRE AS COMUNA,
        UPPER(c.NOMBRE) AS BARRIO,
        CASE a.ETNICO
        WHEN 1 THEN 'BLANCO'
        WHEN 2 THEN 'INDIGENA'
        WHEN 3 THEN 'AFRODECENDIENTE'
        WHEN 4 THEN 'MESTIZO(IND-BLA)'
        WHEN 5 THEN 'MULATO(NEG-BLA)'
        WHEN 6 THEN 'ZAMBO(IND-NEG)'
        WHEN 7 THEN 'GITANO(ROM)'
        WHEN 8 THEN 'RAIZAL(SAN ANDRES)'
        WHEN 9 THEN 'PALENQUERO'
        ELSE 'OTRO'
        END AS ETNICO,
        UPPER(b.NOMBRE) AS PAIS, 
        h.NOMBRE AS CENTRO_PRODUCCION,
        f.CODIGO,
        CASE f.ATENDIDO
        WHEN 0 THEN 'NO'
        WHEN 1 THEN 'SI'
        END AS ATENDIDO,
        DATEADD(DAY, 1, CAST(e.FECHA AS DATE)) AS FECHA,
        DATEADD(DAY, 1, CAST(f.ATENCION_FACTURA AS DATE)) AS ATENCION_FACTURA,
		CASE f.ESTADO
            WHEN  0 THEN 'DISPONIBLE'
            WHEN  1 THEN 'CONFIRMADO'
            WHEN  2 THEN 'INCUMPLIDA'
            WHEN  3 THEN 'CANCELADAS'
			WHEN  4 THEN 'CANCELADA POR EL PRESTADOR'
            END AS ESTADO,
        DATEPART(DAY, a.FECHANAC) AS DIA,
        DATEPART(MONTH, a.FECHANAC) AS MES,
        DATEPART(YEAR, a.FECHANAC) AS AÑO,
        CONCAT(e.CANTEDAD, ' ', 
        CASE e.FORMEDAD
        WHEN 1 THEN 'AÑOS'
        WHEN 2 THEN 'MESES'
        WHEN 3 THEN 'DIAS'
        WHEN 4 THEN 'HORAS'
        END) AS EDAD,
		a.NUMDOCUM AS NUMERO_DOCUMENTO,
		j.CODIGO AS CodigoServicio, 
        CONVERT(varchar, CONVERT(date, f.ATENCION_FACTURA), 120) AS DescripcionServicio
        FROM 
		fac_m_tarjetero a
        JOIN gen_p_paises b ON b.PAIS = a.PAIS 
        JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
        JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
        JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
        JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
        JOIN fac_p_control g ON g.IPS = f.IPS
        JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
        JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
        WHERE h.CODIGO = ${CODIGO} AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal} 
        AND f.ATENCION_FACTURA  IS NOT NULL
        AND f.CODIGO_CUPS IS NOT NULL 
		AND YEAR(f.ATENCION_FACTURA) BETWEEN (YEAR(GETDATE()) - 5) AND YEAR(GETDATE())
			) AS SourceTable
	PIVOT
	(
	MAX(DescripcionServicio)
	FOR CodigoServicio IN (${primeraInfancia})
	) AS PivotTable
        ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}
        ORDER BY a.NUMDOCUM ASC
        OFFSET ${offset} ROWS
        FETCH NEXT ${limit} ROWS ONLY`;
        

        const query1 = `;WITH NumberedResults AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY a.NUMDOCUM ASC) AS RowNum,
                f.IPS,
                g.NOMBRE AS NOMBRE_IPS,
                    CASE a.TIPDOCUM 
                    WHEN 1 THEN 'CEDULA DE CIUDADANIA'
                    WHEN 2 THEN 'TARJETA DE IDENTIDAD'
                    WHEN 3 THEN 'CEDULA DE EXTRANJERIA'
                    WHEN 4 THEN 'REGISTRO CIVIL'
                    WHEN 5 THEN 'PASAPORTE'
                    WHEN 6 THEN 'ADULTO SIN IDENTIFICACION'
                    WHEN 7 THEN 'MENOR SIN IDENTIFICACION'
                    WHEN 9 THEN 'NACIDO VIVO'
                    WHEN 10 THEN 'SALVO CONDUCTO'
                    WHEN 12 THEN 'CARNE DIPLOMATICO'
                    WHEN 13 THEN 'PERMISO ESPECIAL'
                    WHEN 14 THEN 'RECIDENTE ESPECIAL'
                    WHEN 15 THEN 'PERMISO PROTECCION TEMPORAL'
                    ELSE 'OTRO'
                    END AS TIPO_DOCUMENTO,
                    a.POBLACION_ESPECIAL, 
                    a.APELLIDO1 + ' ' + a.APELLIDO2 AS APELLIDOS,
                    a.NOMBRE1 + '  ' + a.NOMBRE2 AS NOMBRES,
                    DATEADD(DAY, 1, CAST(a.FECHANAC AS DATE)) AS FECHA_NACIMIENTO,  
                    CASE a.SEXO 
                    WHEN 2 THEN 'FEMENINO'
                    WHEN 1 THEN 'MASCULINO' 
                    ELSE 'OTRO'
                    END AS GENERO, 
                    CASE e.EMBARAZO 
                    WHEN 0 THEN 'NO'
                    WHEN 1 THEN 'EMBARAZADA' 
                    END AS EMBARAZO, 
                    CASE a.TIPDISCAP 
                    WHEN 1 THEN 'CONDUCTA'
                    WHEN 2 THEN 'COMUNICACION'
                    WHEN 3 THEN 'CUIDADO PERSONAL'
                    WHEN 4 THEN 'LOCOMOCION'
                    WHEN 5 THEN 'DISPOSICION DEL CUERPO'
                    WHEN 6 THEN 'DESTREZA'
                    WHEN 7 THEN 'SITUACION'
                    WHEN 8 THEN 'DETERMINADA ACTITUD'
                    WHEN 9 THEN 'OTRO'
                    END AS TIPO_DISCAPACIDAD, 
                    CASE a.GRDDISCAP 
                    WHEN 1 THEN 'LEVE'
                    WHEN 2 THEN 'MODERADO' 
                    WHEN 3 THEN 'SEVERA'
                    END AS GRADO_DISCAPACIDAD, 
                    a.DIRECRES AS DIRECCION_RESIDENCIA, 
                    a.TELEFRES AS TELEFONO_RESIDENCIA, 
                    a.CODBARES AS CODIGO_BARRIO, 
                    d.NOMBRE AS COMUNA, 
                    UPPER(c.NOMBRE) AS BARRIO, 
                    CASE a.ETNICO 
                    WHEN 1 THEN 'BLANCO'
                    WHEN 2 THEN 'INDIGENA'
                    WHEN 3 THEN 'AFRODECENDIENTE'
                    WHEN 4 THEN 'MESTIZO(IND-BLA)'
                    WHEN 5 THEN 'MULATO(NEG-BLA)'
                    WHEN 6 THEN 'ZAMBO(IND-NEG)'
                    WHEN 7 THEN 'GITANO(ROM)'
                    WHEN 8 THEN 'RAIZAL(SAN ANDRES)'
                    WHEN 9 THEN 'PALENQUERO'
                    ELSE 'OTRO'
                    END AS ETNICO, 
                    UPPER(b.NOMBRE) AS PAIS,  
                    h.NOMBRE AS CENTRO_PRODUCCION, 
                    f.CODIGO, 
                    CASE f.ATENDIDO 
                    WHEN 0 THEN 'NO'
                    WHEN 1 THEN 'SI'
                    END AS ATENDIDO, 
                    DATEADD(DAY, 1, CAST(e.FECHA AS DATE)) AS FECHA, 
                    DATEADD(DAY, 1, CAST(f.ATENCION_FACTURA AS DATE)) AS ATENCION_FACTURA, 
                    CASE f.ESTADO 
                    WHEN  0 THEN 'DISPONIBLE'
                    WHEN  1 THEN 'CONFIRMADO'
                    WHEN  2 THEN 'INCUMPLIDA'
                    WHEN  3 THEN 'CANCELADAS'
                    WHEN  4 THEN 'CANCELADA POR EL PRESTADOR'
                    END AS ESTADO, 
                    DATEPART(DAY, a.FECHANAC) AS DIA, 
                    DATEPART(MONTH, a.FECHANAC) AS MES, 
                    DATEPART(YEAR, a.FECHANAC) AS AÑO, 
                    CONCAT(e.CANTEDAD, + ' ' +
                    CASE e.FORMEDAD 
                    WHEN 1 THEN 'AÑOS'
                    WHEN 2 THEN 'MESES'
                    WHEN 3 THEN 'DIAS'
                    WHEN 4 THEN 'HORAS'
                    END) AS EDAD,  
                    a.NUMDOCUM AS NUMERO_DOCUMENTO,
                    f.CODIGO_CUPS AS DescripcionServicio
                    FROM  fac_m_tarjetero a 
                    JOIN gen_p_paises b ON b.PAIS = a.PAIS  
                    JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
                    JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
                    JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
                    JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
                    JOIN fac_p_control g ON g.IPS = f.IPS 
                    JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD  
                    JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS 
                    WHERE h.CODIGO = ${CODIGO}
                    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
                    AND f.ATENCION_FACTURA IS NOT NULL
                    AND f.CODIGO_CUPS IS NOT NULL
                    AND YEAR(f.ATENCION_FACTURA) BETWEEN (YEAR(GETDATE()) - 5) AND YEAR(GETDATE())
                    ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}
            )
            SELECT DISTINCT
            NOMBRE_IPS,
            TIPO_DOCUMENTO,
            NUMERO_DOCUMENTO,
            APELLIDOS,
            NOMBRES,
            FECHA_NACIMIENTO,
            GENERO,
            EMBARAZO,
            DIRECCION_RESIDENCIA,
            TELEFONO_RESIDENCIA,
            COMUNA,
            BARRIO,
            ETNICO,
            PAIS,
            CODIGO,
            ATENCION_FACTURA,
            ATENDIDO,
            EDAD,
            DescripcionServicio
            FROM NumberedResults
            WHERE RowNum BETWEEN 1 AND 1000; `

        const resul = await sequelize.query(query1, {type: QueryTypes.SELECT});
        /* console.log(resul); */
        
        const queryCount = `SELECT 
        COUNT(*) data
        FROM fac_m_tarjetero a
        JOIN gen_p_paises b ON b.PAIS = a.PAIS 
        JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
        JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
        JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
        JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
        JOIN fac_p_control g ON g.IPS = f.IPS
        JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
        JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
        WHERE h.CODIGO = ${CODIGO} AND floor(
            (cast(convert(varchar(8),getdate(),112) as int)-
            cast(convert(varchar(8),a.FECHANAC,112) as int) ) / 10000
            ) BETWEEN ${edadInicial} AND ${edadFinal} ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}`
        const count = await sequelize.query(queryCount, {type: QueryTypes.SELECT});
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            page,
            totalpage: Math.round(count[0].data / limit),
            itemperpage: limit,
            islast: Math.round(count[0].data / limit) == page,
            total: count[0].data,
            data: resul
        })
        
    } catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
    }
}


// Función para ejecutar la consulta SQL y obtener los resultados
async function executeSQLQuery(CODIGO, edadInicial, edadFinal, offset, limit) {
    const query = `
    SELECT *
    FROM
    (
    SELECT DISTINCT
    f.IPS, 
    g.NOMBRE AS NOMBRE_IPS, 
    CASE a.TIPDOCUM
    WHEN 1 THEN 'CEDULA DE CIUDADANIA'
    WHEN 2 THEN 'TARJETA DE IDENTIDAD'
    WHEN 3 THEN 'CEDULA DE EXTRANJERIA'
    WHEN 4 THEN 'REGISTRO CIVIL'
    WHEN 5 THEN 'PASAPORTE'
    WHEN 6 THEN 'ADULTO SIN IDENTIFICACION'
    WHEN 7 THEN 'MENOR SIN IDENTIFICACION'
    WHEN 9 THEN 'NACIDO VIVO'
    WHEN 10 THEN 'SALVO CONDUCTO'
    WHEN 12 THEN 'CARNE DIPLOMATICO'
    WHEN 13 THEN 'PERMISO ESPECIAL'
    WHEN 14 THEN 'RECIDENTE ESPECIAL'
    WHEN 15 THEN 'PERMISO PROTECCION TEMPORAL'
    ELSE 'OTRO'
    END AS TIPO_DOCUMENTO,
    e.DOCUMENTO AS COD_DOCUMENTO,
    a.POBLACION_ESPECIAL AS IDENT_POBLACION,
    a.APELLIDO1 + '  ' + a.APELLIDO2 AS APELLIDOS,
    a.NOMBRE1 + '  ' + a.NOMBRE2 AS NOMBRES,
    DATEADD(DAY, 1, CAST(a.FECHANAC AS DATE)) AS FECHA_NACIMIENTO, 
    CASE a.SEXO
    WHEN 2 THEN 'FEMENINO'
    WHEN 1 THEN 'MASCULINO' 
    ELSE 'OTRO'
    END AS GENERO,
    CASE e.EMBARAZO
    WHEN 0 THEN 'NO'
    WHEN 1 THEN 'EMBARAZADA' 
    END AS EMBARAZO,
    CASE a.TIPDISCAP
    WHEN 1 THEN 'CONDUCTA'
    WHEN 2 THEN 'COMUNICACION'
    WHEN 3 THEN 'CUIDADO PERSONAL'
    WHEN 4 THEN 'LOCOMOCION'
    WHEN 5 THEN 'DISPOSICION DEL CUERPO'
    WHEN 6 THEN 'DESTREZA'
    WHEN 7 THEN 'SITUACION'
    WHEN 8 THEN 'DETERMINADA ACTITUD'
    WHEN 9 THEN 'OTRO'
    END AS TIPO_DISCAPACIDAD,
    CASE a.GRDDISCAP
    WHEN 1 THEN 'LEVE'
    WHEN 2 THEN 'MODERADO' 
    WHEN 3 THEN 'SEVERA'
    END AS GRADO_DISCAPACIDAD,
    a.DIRECRES AS DIRECCION_RESIDENCIA, 
    a.TELEFRES AS TELEFONO_RESIDENCIA,
    a.CODBARES AS CODIGO_BARRIO,
    d.NOMBRE AS COMUNA,
    UPPER(c.NOMBRE) AS BARRIO,
    CASE a.ETNICO
    WHEN 1 THEN 'BLANCO'
    WHEN 2 THEN 'INDIGENA'
    WHEN 3 THEN 'AFRODECENDIENTE'
    WHEN 4 THEN 'MESTIZO(IND-BLA)'
    WHEN 5 THEN 'MULATO(NEG-BLA)'
    WHEN 6 THEN 'ZAMBO(IND-NEG)'
    WHEN 7 THEN 'GITANO(ROM)'
    WHEN 8 THEN 'RAIZAL(SAN ANDRES)'
    WHEN 9 THEN 'PALENQUERO'
    ELSE 'OTRO'
    END AS ETNICO,
    UPPER(b.NOMBRE) AS PAIS, 
    h.NOMBRE AS CENTRO_PRODUCCION,
    f.CODIGO,
    CASE f.ATENDIDO
    WHEN 0 THEN 'NO'
    WHEN 1 THEN 'SI'
    END AS ATENDIDO,
    CASE f.ESTADO
        WHEN  0 THEN 'DISPONIBLE'
        WHEN  1 THEN 'CONFIRMADO'
        WHEN  2 THEN 'INCUMPLIDA'
        WHEN  3 THEN 'CANCELADAS'
        WHEN  4 THEN 'CANCELADA POR EL PRESTADOR'
        END AS ESTADO,
    DATEPART(DAY, a.FECHANAC) AS DIA,
    DATEPART(MONTH, a.FECHANAC) AS MES,
    DATEPART(YEAR, a.FECHANAC) AS AÑO,
    CONCAT(e.CANTEDAD, ' ', 
    CASE e.FORMEDAD
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    a.NUMDOCUM AS NUMERO_DOCUMENTO,
    j.NOMBRE AS NombreServicio, 
    CONVERT(varchar, CONVERT(date, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio
    FROM 
    fac_m_tarjetero a
    JOIN gen_p_paises b ON b.PAIS = a.PAIS 
    JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
    JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
    JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
    JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
    JOIN fac_p_control g ON g.IPS = f.IPS
    JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
	AND f.ESTADO = 1 
    AND f.ATENCION_FACTURA >= DATEADD(YEAR, -5, GETDATE())
    AND f.ATENCION_FACTURA < GETDATE()
        ) AS SourceTable
PIVOT
(
    MAX(DescripcionServicio)
    FOR NombreServicio IN ( [ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA], [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD], [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO], [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD], [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL], [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL], [CONSULTA DE PRIMERA VEZ POR ENFERMERIA], [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA], [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR], [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN PEDIATRÍA], [CONSULTA DE CONTROL O DE SEGUIMIENTO POR MEDICINA GENERAL], [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL], 
                            [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA], [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA], [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR], [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN PEDIATRÍA], [HEMOGLOBINA], [MEDICION DE AGUDEZA VISUAL], [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL], [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA], [EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA], [EDUCACION GRUPAL EN SALUD, POR ENFERMERIA], [EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA], [EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA], 
                            [EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL], [EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA], [EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL], [EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA], [EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO], [EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL], [EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL], [EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA], [EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA], [EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA], [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL], [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA], 
                            [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL], [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL], [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO], [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN INFANTIL Y ADOLESCENTE], [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL], [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES], [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR], [VACUNACION CONTRA TUBERCULOSIS BCG], 
                            [VACUNACIÓN CONTRA NEUMOCOCO], [VACUNACIÓN COMBINADA CONTRA DIFTERIA, TÉTANOS Y TOS FERINA DPT], [VACUNACIÓN COMBINADA CONTRA HAEMOPHILUS INFLUENZA TIPO B, DIFTERIA, TÉTANOS, TOS FERINA Y HEPATITIS B PENTAVALENTE], [VACUNACION CONTRA POLIOMIELITIS (VOP O IVP)], [VACUNACIÓN CONTRA POLIOMIELITIS (VOP O IVP) INTRAMUSCULAR], [VACUNACION CONTRA HEPATITIS A], [VACUNACION CONTRA Hepatitis B], [VACUNACION CONTRA FIEBRE AMARILLA], [VACUNACION CONTRA VARICELA], [VACUNA CONTRA INFLUENZA PEDIATRICA  (H1N1)], [VACUNACION CONTRA ROTAVIRUS], [VACUNACIÓN COMBINADA CONTRA SARAMPIÓN, PAROTIDITIS Y RUBÉOLA SRP TRIPLE VIRAL], [APLICACION DE SELLANTES DE FOTOCURADO], [TOPICACIÓN DE FLUOR EN BARNIZ], [CONTROL DE PLACA DENTAL -], 
                            [CONSEJERIA EN VIH], [DEMANDA INDUCIDA CANALIZADOS ALTERACIONES DE LA AGUDEZA VISUAL], [DEMANDA INDUCIDA CANALIZADOS ATENCION DEL PARTO], [DEMANDA INDUCIDA CANALIZADOS CRECIMIENTO Y DESARROLLO], [DEMANDA INDUCIDA CANALIZADOS VACUNACION PAI], [DEMANDA INDUCIDA CANALIZADOS VALORACION DEL JOVEN])
    ) AS PivotTable
    ORDER BY NUMERO_DOCUMENTO ASC
    OFFSET ${offset} ROWS
    FETCH NEXT ${limit} ROWS ONLY`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results) {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Datos');

    // Obtener los nombres de las columnas desde los resultados de la consulta
    const columns = Object.keys(results[0]);

        // Separar las columnas en dos grupos: con números y sin números
        const columnsWithNumbers = columns.filter((column) => /\d/.test(column));
        const columnsWithoutNumbers = columns.filter((column) => !/\d/.test(column));
    
        // Configurar las columnas en el archivo de Excel
        const sortedColumns = [...columnsWithoutNumbers, ...columnsWithNumbers];
    
        worksheet.columns = sortedColumns.map((column) => {
            return { header: column, key: column, width: 40 };
        });

         // Agregar color y filtro a los encabezados
        worksheet.getRow(1).eachCell((cell) => {
        cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '87CEEB' }, // Color de fondo
        };
    });
    
        // Agregar los datos a la hoja de cálculo
        results.forEach((item) => {
            const rowData = [];
            sortedColumns.forEach((column) => {
                rowData.push(item[column]);
            });
            worksheet.addRow(rowData);
        });
    
        // Centrar el contenido de las celdas
        worksheet.eachRow((row) => {
            row.eachCell((cell) => {
                cell.alignment = { vertical: 'middle', horizontal: 'center' };
            });
        });

        // Agregar filtros a los encabezados de las columnas
        worksheet.autoFilter = {
            from: { row: 1, column: 1 },
            to: { row: 1, column: sortedColumns.length }
        };
        // Agregar bordes a todas las filas y columnas
        worksheet.eachRow((row) => {
            row.eachCell((cell) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' },
                };
            });
        });
    
        return workbook;
    }
    // Función principal que maneja la solicitud y la respuesta
    exports.cursoVidaExcel = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const page = req.query.page || 1;
        const limit = req.query.limit || 10000;
        const offset = (page - 1) * limit;

        // Ejecutar la consulta SQL
        const results = await executeSQLQuery(CODIGO, edadInicial, edadFinal, offset, limit);

        // Configurar el libro de Excel
        const workbook = configureExcelWorkbook(results);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            'attachment; filename=datos.xlsx'
        );

        // Escribir el archivo de Excel en la respuesta
        await workbook.xlsx.write(res);

        // Finalizar la respuesta
        res.end();
    } catch (error) {
        console.error(error.message);
        res.status(500).send('Error en el servidor');
    }
};

// Función para ejecutar la consulta SQL y obtener los resultados
async function executeSQLQuery1(CODIGO, edadInicial, edadFinal, offset, limit) {
    const query = `
    SELECT *
    FROM
    (
    SELECT DISTINCT
    f.IPS, 
    g.NOMBRE AS NOMBRE_IPS, 
    CASE a.TIPDOCUM
    WHEN 1 THEN 'CEDULA DE CIUDADANIA'
    WHEN 2 THEN 'TARJETA DE IDENTIDAD'
    WHEN 3 THEN 'CEDULA DE EXTRANJERIA'
    WHEN 4 THEN 'REGISTRO CIVIL'
    WHEN 5 THEN 'PASAPORTE'
    WHEN 6 THEN 'ADULTO SIN IDENTIFICACION'
    WHEN 7 THEN 'MENOR SIN IDENTIFICACION'
    WHEN 9 THEN 'NACIDO VIVO'
    WHEN 10 THEN 'SALVO CONDUCTO'
    WHEN 12 THEN 'CARNE DIPLOMATICO'
    WHEN 13 THEN 'PERMISO ESPECIAL'
    WHEN 14 THEN 'RECIDENTE ESPECIAL'
    WHEN 15 THEN 'PERMISO PROTECCION TEMPORAL'
    ELSE 'OTRO'
    END AS TIPO_DOCUMENTO,
    e.DOCUMENTO AS COD_DOCUMENTO,
    a.POBLACION_ESPECIAL AS IDENT_POBLACION,
    a.APELLIDO1 + '  ' + a.APELLIDO2 AS APELLIDOS,
    a.NOMBRE1 + '  ' + a.NOMBRE2 AS NOMBRES,
    DATEADD(DAY, 1, CAST(a.FECHANAC AS DATE)) AS FECHA_NACIMIENTO, 
    CASE a.SEXO
    WHEN 2 THEN 'FEMENINO'
    WHEN 1 THEN 'MASCULINO' 
    ELSE 'OTRO'
    END AS GENERO,
    CASE e.EMBARAZO
    WHEN 0 THEN 'NO'
    WHEN 1 THEN 'EMBARAZADA' 
    END AS EMBARAZO,
    CASE a.TIPDISCAP
    WHEN 1 THEN 'CONDUCTA'
    WHEN 2 THEN 'COMUNICACION'
    WHEN 3 THEN 'CUIDADO PERSONAL'
    WHEN 4 THEN 'LOCOMOCION'
    WHEN 5 THEN 'DISPOSICION DEL CUERPO'
    WHEN 6 THEN 'DESTREZA'
    WHEN 7 THEN 'SITUACION'
    WHEN 8 THEN 'DETERMINADA ACTITUD'
    WHEN 9 THEN 'OTRO'
    END AS TIPO_DISCAPACIDAD,
    CASE a.GRDDISCAP
    WHEN 1 THEN 'LEVE'
    WHEN 2 THEN 'MODERADO' 
    WHEN 3 THEN 'SEVERA'
    END AS GRADO_DISCAPACIDAD,
    a.DIRECRES AS DIRECCION_RECIDENCIA, 
    a.TELEFRES AS TELEFONO_RECIDENCIA,
    a.CODBARES AS CODIGO_BARRIO,
    d.NOMBRE AS COMUNA,
    UPPER(c.NOMBRE) AS BARRIO,
    CASE a.ETNICO
    WHEN 1 THEN 'BLANCO'
    WHEN 2 THEN 'INDIGENA'
    WHEN 3 THEN 'AFRODECENDIENTE'
    WHEN 4 THEN 'MESTIZO(IND-BLA)'
    WHEN 5 THEN 'MULATO(NEG-BLA)'
    WHEN 6 THEN 'ZAMBO(IND-NEG)'
    WHEN 7 THEN 'GITANO(ROM)'
    WHEN 8 THEN 'RAIZAL(SAN ANDRES)'
    WHEN 9 THEN 'PALENQUERO'
    ELSE 'OTRO'
    END AS ETNICO,
    UPPER(b.NOMBRE) AS PAIS, 
    h.NOMBRE AS CENTRO_PRODUCCION,
    f.CODIGO,
    CASE f.ATENDIDO
    WHEN 0 THEN 'NO'
    WHEN 1 THEN 'SI'
    END AS ATENDIDO,
    CASE f.ESTADO
        WHEN  0 THEN 'DISPONIBLE'
        WHEN  1 THEN 'CONFIRMADO'
        WHEN  2 THEN 'INCUMPLIDA'
        WHEN  3 THEN 'CANCELADAS'
        WHEN  4 THEN 'CANCELADA POR EL PRESTADOR'
        END AS ESTADO,
    DATEPART(DAY, a.FECHANAC) AS DIA,
    DATEPART(MONTH, a.FECHANAC) AS MES,
    DATEPART(YEAR, a.FECHANAC) AS AÑO,
    CONCAT(e.CANTEDAD, ' ', 
    CASE e.FORMEDAD
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    a.NUMDOCUM AS NUMERO_DOCUMENTO,
    j.CODIGO AS CodigoServicio, 
    CONVERT(varchar, CONVERT(date, f.ATENCION_FACTURA), 120) + '  //  '  + j.NOMBRE AS DescripcionServicio
    FROM 
    fac_m_tarjetero a
    JOIN gen_p_paises b ON b.PAIS = a.PAIS 
    JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
    JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
    JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
    JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
    JOIN fac_p_control g ON g.IPS = f.IPS
    JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL 
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENCION_FACTURA >= DATEADD(YEAR, -5, GETDATE())
    AND f.ATENCION_FACTURA < GETDATE()
        ) AS SourceTable
PIVOT
(
MAX(DescripcionServicio)
FOR CodigoServicio IN ([890105], [890114], [890115], [890116], [890201], [890203], [890205], [890206], [890263], [890283], [890301], [890303], 
                        [890305], [890306], [890363], [890383], [902211], [902213], [950601], [990101], [990102], [990103], [990104], [990105], 
                        [990106], [990107], [990108], [990109], [990110], [990111], [990112], [990201], [990203], [990204], [990206], [990207],  
                        [990208], [990209], [990212], [990213], [990222], [990224], [993120], [993510], [993513], [993522], [997102], [997106],
                        [997310], [P0000002], [P0000004], [P0000006], [P0000009], [P0000013], [P0000014], [P0001504])
) AS PivotTable
ORDER BY NUMERO_DOCUMENTO ASC
    OFFSET ${offset} ROWS
    FETCH NEXT ${limit} ROWS ONLY`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results) {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Datos');

    // Obtener los nombres de las columnas desde los resultados de la consulta
    const columns = Object.keys(results[0]);

        // Separar las columnas en dos grupos: con números y sin números
        const columnsWithNumbers = columns.filter((column) => /\d/.test(column));
        const columnsWithoutNumbers = columns.filter((column) => !/\d/.test(column));
    
        // Configurar las columnas en el archivo de Excel
        const sortedColumns = [...columnsWithoutNumbers, ...columnsWithNumbers];
    
        worksheet.columns = sortedColumns.map((column) => {
            return { header: column, key: column, width: 40 };
        });

         // Agregar color y filtro a los encabezados
        worksheet.getRow(1).eachCell((cell) => {
        cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '87CEEB' }, // Color de fondo
        };
    });
    
        // Agregar los datos a la hoja de cálculo
        results.forEach((item) => {
            const rowData = [];
            sortedColumns.forEach((column) => {
                rowData.push(item[column]);
            });
            worksheet.addRow(rowData);
        });
    
        // Centrar el contenido de las celdas
        worksheet.eachRow((row) => {
            row.eachCell((cell) => {
                cell.alignment = { vertical: 'middle', horizontal: 'center' };
            });
        });

        // Agregar filtros a los encabezados de las columnas
        worksheet.autoFilter = {
            from: { row: 1, column: 1 },
            to: { row: 1, column: sortedColumns.length }
        };
        // Agregar bordes a todas las filas y columnas
        worksheet.eachRow((row) => {
            row.eachCell((cell) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' },
                };
            });
        });
    
        return workbook;
    }


    // Función principal que maneja la solicitud y la respuesta
    exports.cursoVidaExcel1 = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const page = req.query.page || 1;
        const limit = req.query.limit || 10000;
        const offset = (page - 1) * limit;

        // Ejecutar la consulta SQL
        const results = await executeSQLQuery1(CODIGO, edadInicial, edadFinal, offset, limit);

        // Configurar el libro de Excel
        const workbook = configureExcelWorkbook(results);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            'attachment; filename=datos.xlsx'
        );

        // Escribir el archivo de Excel en la respuesta
        await workbook.xlsx.write(res);

        // Finalizar la respuesta
        res.end();
    } catch (error) {
        console.error(error.message);
        res.status(500).send('Error en el servidor');
    }
};

// Función para ejecutar la consulta SQL y obtener los resultados
async function executeSQLQuery2(CODIGO, edadInicial, edadFinal, offset, limit) {
    const query = `
    SELECT *
    FROM
    (
    SELECT DISTINCT
    f.IPS, 
    g.NOMBRE AS NOMBRE_IPS, 
    CASE a.TIPDOCUM
    WHEN 1 THEN 'CEDULA DE CIUDADANIA'
    WHEN 2 THEN 'TARJETA DE IDENTIDAD'
    WHEN 3 THEN 'CEDULA DE EXTRANJERIA'
    WHEN 4 THEN 'REGISTRO CIVIL'
    WHEN 5 THEN 'PASAPORTE'
    WHEN 6 THEN 'ADULTO SIN IDENTIFICACION'
    WHEN 7 THEN 'MENOR SIN IDENTIFICACION'
    WHEN 9 THEN 'NACIDO VIVO'
    WHEN 10 THEN 'SALVO CONDUCTO'
    WHEN 12 THEN 'CARNE DIPLOMATICO'
    WHEN 13 THEN 'PERMISO ESPECIAL'
    WHEN 14 THEN 'RECIDENTE ESPECIAL'
    WHEN 15 THEN 'PERMISO PROTECCION TEMPORAL'
    ELSE 'OTRO'
    END AS TIPO_DOCUMENTO,
    e.DOCUMENTO AS COD_DOCUMENTO,
    a.POBLACION_ESPECIAL AS IDENT_POBLACION,
    a.APELLIDO1 + '  ' + a.APELLIDO2 AS APELLIDOS,
    a.NOMBRE1 + '  ' + a.NOMBRE2 AS NOMBRES,
    DATEADD(DAY, 1, CAST(a.FECHANAC AS DATE)) AS FECHA_NACIMIENTO, 
    CASE a.SEXO
    WHEN 2 THEN 'FEMENINO'
    WHEN 1 THEN 'MASCULINO' 
    ELSE 'OTRO'
    END AS GENERO,
    CASE e.EMBARAZO
    WHEN 0 THEN 'NO'
    WHEN 1 THEN 'EMBARAZADA' 
    END AS EMBARAZO,
    CASE a.TIPDISCAP
    WHEN 1 THEN 'CONDUCTA'
    WHEN 2 THEN 'COMUNICACION'
    WHEN 3 THEN 'CUIDADO PERSONAL'
    WHEN 4 THEN 'LOCOMOCION'
    WHEN 5 THEN 'DISPOSICION DEL CUERPO'
    WHEN 6 THEN 'DESTREZA'
    WHEN 7 THEN 'SITUACION'
    WHEN 8 THEN 'DETERMINADA ACTITUD'
    WHEN 9 THEN 'OTRO'
    END AS TIPO_DISCAPACIDAD,
    CASE a.GRDDISCAP
    WHEN 1 THEN 'LEVE'
    WHEN 2 THEN 'MODERADO' 
    WHEN 3 THEN 'SEVERA'
    END AS GRADO_DISCAPACIDAD,
    a.DIRECRES AS DIRECCION_RECIDENCIA, 
    a.TELEFRES AS TELEFONO_RECIDENCIA,
    a.CODBARES AS CODIGO_BARRIO,
    d.NOMBRE AS COMUNA,
    UPPER(c.NOMBRE) AS BARRIO,
    CASE a.ETNICO
    WHEN 1 THEN 'BLANCO'
    WHEN 2 THEN 'INDIGENA'
    WHEN 3 THEN 'AFRODECENDIENTE'
    WHEN 4 THEN 'MESTIZO(IND-BLA)'
    WHEN 5 THEN 'MULATO(NEG-BLA)'
    WHEN 6 THEN 'ZAMBO(IND-NEG)'
    WHEN 7 THEN 'GITANO(ROM)'
    WHEN 8 THEN 'RAIZAL(SAN ANDRES)'
    WHEN 9 THEN 'PALENQUERO'
    ELSE 'OTRO'
    END AS ETNICO,
    UPPER(b.NOMBRE) AS PAIS, 
    h.NOMBRE AS CENTRO_PRODUCCION,
    f.CODIGO,
    CASE f.ATENDIDO
    WHEN 0 THEN 'NO'
    WHEN 1 THEN 'SI'
    END AS ATENDIDO,
    CASE f.ESTADO
        WHEN  0 THEN 'DISPONIBLE'
        WHEN  1 THEN 'CONFIRMADO'
        WHEN  2 THEN 'INCUMPLIDA'
        WHEN  3 THEN 'CANCELADAS'
        WHEN  4 THEN 'CANCELADA POR EL PRESTADOR'
        END AS ESTADO,
    DATEPART(DAY, a.FECHANAC) AS DIA,
    DATEPART(MONTH, a.FECHANAC) AS MES,
    DATEPART(YEAR, a.FECHANAC) AS AÑO,
    CONCAT(e.CANTEDAD, ' ', 
    CASE e.FORMEDAD
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    a.NUMDOCUM AS NUMERO_DOCUMENTO,
    j.CODIGO AS CodigoServicio, 
    CONVERT(varchar, CONVERT(date, f.ATENCION_FACTURA), 120) + '  //  '  + j.NOMBRE AS DescripcionServicio
    FROM 
    fac_m_tarjetero a
    JOIN gen_p_paises b ON b.PAIS = a.PAIS 
    JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
    JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
    JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
    JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
    JOIN fac_p_control g ON g.IPS = f.IPS
    JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL 
    AND f.ATENCION_FACTURA >= DATEADD(YEAR, -5, GETDATE())
    AND f.ATENCION_FACTURA < GETDATE()
        ) AS SourceTable
PIVOT
(
MAX(DescripcionServicio)
FOR CodigoServicio IN ( [697100], [702201], [861801], [890105], [890114], [890115], [890116], [890201], [890203], [890205], [890206], [890250],  
                        [890263], [890283], [890301], [890303], [890305], [890306], [890350], [890363], [890383], [892901], [898001], [898101], 
                        [902211], [902213], [904508], [906039], [90603901], [906249], [90624901], [950601], [990101], [990102], [990103], [990104], 
                        [990105], [990106], [990107], [990108], [990109], [990110], [990111], [990112], [990201], [990203], [990204], [99020401], 
                        [99020402], [99020406], [990206], [990207],  [990208], [990209], [990212], [990213], [990222], [990223], [990224], [993120],
                        [99312202], [993513], [997102], [997106], [997301], [997310], [P0000002], [P0000004], [P0000006], [P0000009], [P0000013], 
                        [P0000014], [P0000775], [P0001504])
) AS PivotTable
ORDER BY NUMERO_DOCUMENTO ASC
    OFFSET ${offset} ROWS
    FETCH NEXT ${limit} ROWS ONLY`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results) {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Datos');

    // Obtener los nombres de las columnas desde los resultados de la consulta
    const columns = Object.keys(results[0]);

        // Separar las columnas en dos grupos: con números y sin números
        const columnsWithNumbers = columns.filter((column) => /\d/.test(column));
        const columnsWithoutNumbers = columns.filter((column) => !/\d/.test(column));
    
        // Configurar las columnas en el archivo de Excel
        const sortedColumns = [...columnsWithoutNumbers, ...columnsWithNumbers];
    
        worksheet.columns = sortedColumns.map((column) => {
            return { header: column, key: column, width: 40 };
        });

         // Agregar color y filtro a los encabezados
        worksheet.getRow(1).eachCell((cell) => {
        cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '87CEEB' }, // Color de fondo
        };
    });
    
        // Agregar los datos a la hoja de cálculo
        results.forEach((item) => {
            const rowData = [];
            sortedColumns.forEach((column) => {
                rowData.push(item[column]);
            });
            worksheet.addRow(rowData);
        });
    
        // Centrar el contenido de las celdas
        worksheet.eachRow((row) => {
            row.eachCell((cell) => {
                cell.alignment = { vertical: 'middle', horizontal: 'center' };
            });
        });

        // Agregar filtros a los encabezados de las columnas
        worksheet.autoFilter = {
            from: { row: 1, column: 1 },
            to: { row: 1, column: sortedColumns.length }
        };
        // Agregar bordes a todas las filas y columnas
        worksheet.eachRow((row) => {
            row.eachCell((cell) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' },
                };
            });
        });
    
        return workbook;
    }
    // Función principal que maneja la solicitud y la respuesta
    exports.cursoVidaExcel2 = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const page = req.query.page || 1;
        const limit = req.query.limit || 10000;
        const offset = (page - 1) * limit;

        // Ejecutar la consulta SQL
        const results = await executeSQLQuery2(CODIGO, edadInicial, edadFinal, offset, limit);

        // Configurar el libro de Excel
        const workbook = configureExcelWorkbook(results);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            'attachment; filename=datos.xlsx'
        );

        // Escribir el archivo de Excel en la respuesta
        await workbook.xlsx.write(res);

        // Finalizar la respuesta
        res.end();
    } catch (error) {
        console.error(error.message);
        res.status(500).send('Error en el servidor');
    }
};
