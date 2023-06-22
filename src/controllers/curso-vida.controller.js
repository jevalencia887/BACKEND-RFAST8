const { QueryTypes, and } = require('sequelize');
const {sequelize} = require('../BD-RFAS8/conexiondb');
const ExcelJS = require('exceljs');

exports.cursoVida = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO; 
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const page = req.query.page || 1;
        const limit = req.query.limit || 10;
        const offset = (page - 1) * limit;
        const query = `SELECT DISTINCT a.NUMDOCUM,
        f.IPS, 
        g.NOMBRE AS NOMBRE_IPS, 
        CASE
        WHEN a.TIPDOCUM = 1 THEN 'CEDULA DE CIUDADANIA'
        WHEN a.TIPDOCUM = 2 THEN 'TARJETA DE IDENTIDAD'
        WHEN a.TIPDOCUM = 3 THEN 'CEDULA DE EXTRANJERIA'
        WHEN a.TIPDOCUM = 4 THEN 'REGISTRO CIVIL'
        WHEN a.TIPDOCUM = 5 THEN 'PASAPORTE'
		WHEN a.TIPDOCUM = 6 THEN 'ADULTO SIN IDENTIFICACION'
		WHEN a.TIPDOCUM = 7 THEN 'MENOR SIN IDENTIFICACION'
		WHEN a.TIPDOCUM = 9 THEN 'NACIDO VIVO'
		WHEN a.TIPDOCUM = 10 THEN 'SALVO CONDUCTO'
		WHEN a.TIPDOCUM = 12 THEN 'CARNE DIPLOMATICO'
		WHEN a.TIPDOCUM = 13 THEN 'PERMISO ESPECIAL'
		WHEN a.TIPDOCUM = 14 THEN 'RECIDENTE ESPECIAL'
		WHEN a.TIPDOCUM = 15 THEN 'PERMISO PROTECCION TEMPORAL'
        ELSE 'OTRO'
        END AS TIPO_DOCUMENTO, 
        a.NUMDOCUM AS NUMERO_DOCUMENTO, 
        a.POBLACION_ESPECIAL,
        a.APELLIDO1 + ' ' + a.APELLIDO2 AS APELLIDOS,
        a.NOMBRE1 + ' ' + a.NOMBRE2 AS NOMBRES,
        CONVERT(date, a.FECHANAC, 120) AS FECHA_NACIMIENTO, 
        CASE
		WHEN a.SEXO = 2 THEN 'FEMENINO'
		WHEN a.SEXO = 1 THEN 'MASCULINO' 
		ELSE 'OTRO'
		END AS SEXO,
        CASE
		WHEN e.EMBARAZO = 0 THEN 'NO'
		WHEN e.EMBARAZO = 1 THEN 'EMBARAZADA' 
		END AS EMBARAZO,
        CASE
		WHEN a.TIPDISCAP = 1 THEN 'CONDUCTA'
		WHEN a.TIPDISCAP = 2 THEN 'COMUNICACION'
		WHEN a.TIPDISCAP = 3 THEN 'CUIDADO PERSONAL'
		WHEN a.TIPDISCAP = 4 THEN 'LOCOMOCION'
		WHEN a.TIPDISCAP = 5 THEN 'DISPOSICION DEL CUERPO'
		WHEN a.TIPDISCAP = 6 THEN 'DESTREZA'
		WHEN a.TIPDISCAP = 7 THEN 'SITUACION'
		WHEN a.TIPDISCAP = 8 THEN 'DETERMINADA ACTITUD'
		WHEN a.TIPDISCAP = 9 THEN 'OTRO'
		END AS TIPO_DISCAPACIDAD,
        CASE
		WHEN a.GRDDISCAP = 1 THEN 'LEVE'
		WHEN a.GRDDISCAP = 2 THEN 'MODERADO' 
		WHEN a.GRDDISCAP = 3 THEN 'SEVERA'
		END AS GRADO_DISCAPACIDAD, 
        a.DIRECRES AS DIRECCION_RECIDENCIA, 
        a.TELEFRES AS TELEFONO_RECIDENCIA,
        a.CODBARES AS CODIGO_BARRIO,
        d.NOMBRE AS COMUNA,
        c.NOMBRE AS BARRIO,
        CASE
		WHEN a.ETNICO = 1 THEN 'BLANCO'
		WHEN a.ETNICO = 2 THEN 'INDIGENA'
		WHEN a.ETNICO = 3 THEN 'AFRODECENDIENTE'
		WHEN a.ETNICO = 4 THEN 'MESTIZO(IND-BLA)'
		WHEN a.ETNICO = 5 THEN 'MULATO(NEG-BLA)'
		WHEN a.ETNICO = 6 THEN 'ZAMBO(IND-NEG)'
		WHEN a.ETNICO = 7 THEN 'GITANO(ROM)'
		WHEN a.ETNICO = 8 THEN 'RAIZAL(SAN ANDRES)'
		WHEN a.ETNICO = 9 THEN 'PALENQUERO'
		ELSE 'OTRO'
		END AS ETNICO, 
        b.NOMBRE AS PAIS, 
        h.NOMBRE, 
        j.NOMBRE AS CUPS, 
        CONVERT(date, f.ATENCION_FACTURA, 120) AS ATENCION_FACTURA, 
        f.CODIGO, 
        CASE
        WHEN f.ATENDIDO = 0 THEN 'NO'
		WHEN f.ATENDIDO = 1 THEN 'SI'
		END AS ATENDIDO,
		CONCAT(
			CAST(
				FLOOR(
					(CAST(CONVERT(VARCHAR(8), GETDATE(), 112) AS INT) -
					CAST(CONVERT(VARCHAR(8), a.FECHANAC, 112) AS INT)) / 10000
				) AS VARCHAR(10)
			),
			' ',
			CASE e.FORMEDAD
				WHEN 1 THEN 'AÑOS'
				WHEN 2 THEN 'MESES'
				WHEN 3 THEN 'DIAS'
				WHEN 4 THEN 'HORAS'
			END
		) AS EDAD
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
            ) BETWEEN ${edadInicial} AND ${edadFinal} AND f.ATENCION_FACTURA  IS NOT NULL
        ORDER BY a.NUMDOCUM ASC
        OFFSET ${offset} ROWS
        FETCH NEXT ${limit} ROWS ONLY`;
        const resul = await sequelize.query(query, {type: QueryTypes.SELECT});
        
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
            ) BETWEEN ${edadInicial} AND ${edadFinal}`
        const count = await sequelize.query(queryCount, {type: QueryTypes.SELECT});
        
        const queryAtendidos = `SELECT
        COUNT(*) atendidos
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
            ) BETWEEN ${edadInicial} AND ${edadFinal} and f.ATENDIDO = 1`
        const atendidos = await sequelize.query(queryAtendidos, {type: QueryTypes.SELECT});
        
        const querySinAtencion = `SELECT
        COUNT(*) noAtendidos
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
            ) BETWEEN ${edadInicial} AND ${edadFinal} and f.ATENDIDO = 0`
        const noAtendidos = await sequelize.query(querySinAtencion, {type: QueryTypes.SELECT});
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            page,
            totalpage: Math.round(count[0].data / limit),
            itemperpage: limit,
            islast: Math.round(count[0].data / limit) == page,
            atendidos,
            noAtendidos,
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
        const page = req.query.page || 1;
        const limit = req.query.limit || 10;
        const offset = (page - 1) * limit;
        const arrayId = req.body;
        console.log(req.body);
        let it = ""
        for (let i = 0; i < arrayId.length; i++) {
            const id = arrayId[i];
            if(i == 0){
                it +=  `AND a.NUMDOCUM = '${id}' `
            }
            else{
                it +=  `OR a.NUMDOCUM = '${id}' `
            }
            
            
        }
        
        const query = `SELECT DISTINCT a.NUMDOCUM,
        f.IPS, g.NOMBRE AS NOMBRE_IPS, 
        CASE
        WHEN a.TIPDOCUM = 1 THEN 'CEDULA DE CIUDADANIA'
        WHEN a.TIPDOCUM = 2 THEN 'TARJETA DE IDENTIDAD'
        WHEN a.TIPDOCUM = 3 THEN 'CEDULA DE EXTRANJERIA'
        WHEN a.TIPDOCUM = 4 THEN 'REGISTRO CIVIL'
        WHEN a.TIPDOCUM = 5 THEN 'PASAPORTE'
		WHEN a.TIPDOCUM = 6 THEN 'ADULTO SIN IDENTIFICACION'
		WHEN a.TIPDOCUM = 7 THEN 'MENOR SIN IDENTIFICACION'
        WHEN a.TIPDOCUM = 11 THEN 'CARNE DIPLOMATICO'
		WHEN a.TIPDOCUM = 12 THEN 'NACIDO VIVO'
		WHEN a.TIPDOCUM = 13 THEN 'SALVO CONDUCTO'
		WHEN a.TIPDOCUM = 14 THEN 'PASAPORTE'
        ELSE 'OTRO'
        END AS TIPO_DOCUMENTO, 
        a.NUMDOCUM AS NUMERO_DOCUMENTO, 
        a.POBLACION_ESPECIAL,
        a.APELLIDO1, 
        a.APELLIDO2, 
        a.NOMBRE1,
        a.NOMBRE2,
        CONVERT(date, a.FECHANAC, 120) AS FECHA_NACIMIENTO, 
        CASE
		WHEN a.SEXO = 2 THEN 'FEMENINO'
		WHEN a.SEXO = 1 THEN 'MASCULINO' 
		ELSE 'OTRO'
		END AS SEXO,
        CASE
		WHEN e.EMBARAZO = 0 THEN 'NO'
		WHEN e.EMBARAZO = 1 THEN 'EMBARAZADA' 
		END AS EMBARAZO,
        CASE
		WHEN a.TIPDISCAP = 1 THEN 'CONDUCTA'
		WHEN a.TIPDISCAP = 2 THEN 'COMUNICACION'
		WHEN a.TIPDISCAP = 3 THEN 'CUIDADO PERSONAL'
		WHEN a.TIPDISCAP = 4 THEN 'LOCOMOCION'
		WHEN a.TIPDISCAP = 5 THEN 'DISPOSICION DEL CUERPO'
		WHEN a.TIPDISCAP = 6 THEN 'DESTREZA'
		WHEN a.TIPDISCAP = 7 THEN 'SITUACION'
		WHEN a.TIPDISCAP = 8 THEN 'DETERMINADA ACTITUD'
		WHEN a.TIPDISCAP = 9 THEN 'OTRO'
		END AS TIPO_DISCAPACIDAD,
        CASE
		WHEN a.GRDDISCAP = 1 THEN 'LEVE'
		WHEN a.GRDDISCAP = 2 THEN 'MODERADO' 
		WHEN a.GRDDISCAP = 3 THEN 'SEVERA'
		END AS GRADO_DISCAPACIDAD, 
        a.DIRECRES AS DIRECCION_RECIDENCIA, 
        a.TELEFRES AS TELEFONO_RECIDENCIA,
        a.CODBARES AS CODIGO_BARRIO,
        d.NOMBRE AS COMUNA,
        c.NOMBRE AS BARRIO,
        CASE
		WHEN a.ETNICO = 1 THEN 'BLANCO'
		WHEN a.ETNICO = 2 THEN 'INDIGENA'
		WHEN a.ETNICO = 3 THEN 'AFRODECENDIENTE'
		WHEN a.ETNICO = 4 THEN 'MESTIZO(IND-BLA)'
		WHEN a.ETNICO = 5 THEN 'MULATO(NEG-BLA)'
		WHEN a.ETNICO = 6 THEN 'ZAMBO(IND-NEG)'
		WHEN a.ETNICO = 7 THEN 'GITANO(ROM)'
		WHEN a.ETNICO = 8 THEN 'RAIZAL(SAN ANDRES)'
		WHEN a.ETNICO = 9 THEN 'PALENQUERO'
		ELSE 'OTRO'
		END AS ETNICO, 
        b.NOMBRE AS PAIS, 
        h.NOMBRE, 
        j.NOMBRE AS CUPS, 
        CONVERT(date, f.ATENCION_FACTURA, 120) AS ATENCION_FACTURA, 
        f.CODIGO, 
        CASE
        WHEN f.ATENDIDO = 0 THEN 'NO'
		WHEN f.ATENDIDO = 1 THEN 'SI'
		END AS ATENDIDO,
        floor(
            (cast(convert(varchar(8),getdate(),112) as int)-
            cast(convert(varchar(8),a.FECHANAC,112) as int) ) / 10000
            ) as EDAD 
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
            ) BETWEEN ${edadInicial} AND ${edadFinal} AND f.ATENCION_FACTURA  IS NOT NULL
        ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}
        ORDER BY a.NUMDOCUM ASC
        OFFSET ${offset} ROWS
        FETCH NEXT ${limit} ROWS ONLY`;
        const resul = await sequelize.query(query, {type: QueryTypes.SELECT});
        
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
        
        const queryAtendidos = `SELECT
        COUNT(*) atendidos
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
            ) BETWEEN ${edadInicial} AND ${edadFinal} AND f.ATENDIDO = 1 ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}`
        const atendidos = await sequelize.query(queryAtendidos, {type: QueryTypes.SELECT});

        const querySinAtencion = `SELECT
        COUNT(*) noAtendidos
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
            ) BETWEEN ${edadInicial} AND ${edadFinal} AND f.ATENDIDO = 0 ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}`
        const noAtendidos = await sequelize.query(querySinAtencion, {type: QueryTypes.SELECT});
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            page,
            totalpage: Math.round(count[0].data / limit),
            itemperpage: limit,
            islast: Math.round(count[0].data / limit) == page,
            atendidos,
            noAtendidos,
            total: count[0].data,
            data: resul
        })
        
    } catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
    }
}


exports.cursoVidaExcel = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO; 
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const query = 
        ` 
        SELECT DISTINCT TOP(3650) a.NUMDOCUM,
        f.IPS, 
        g.NOMBRE AS NOMBRE_IPS, 
        CASE
        WHEN a.TIPDOCUM = 1 THEN 'CEDULA DE CIUDADANIA'
        WHEN a.TIPDOCUM = 2 THEN 'TARJETA DE IDENTIDAD'
        WHEN a.TIPDOCUM = 3 THEN 'CEDULA DE EXTRANJERIA'
        WHEN a.TIPDOCUM = 4 THEN 'REGISTRO CIVIL'
        WHEN a.TIPDOCUM = 5 THEN 'PASAPORTE'
        WHEN a.TIPDOCUM = 6 THEN 'ADULTO SIN IDENTIFICACION'
        WHEN a.TIPDOCUM = 7 THEN 'MENOR SIN IDENTIFICACION'
        WHEN a.TIPDOCUM = 9 THEN 'NACIDO VIVO'
        WHEN a.TIPDOCUM = 10 THEN 'SALVO CONDUCTO'
        WHEN a.TIPDOCUM = 12 THEN 'CARNE DIPLOMATICO'
        WHEN a.TIPDOCUM = 13 THEN 'PERMISO ESPECIAL'
        WHEN a.TIPDOCUM = 14 THEN 'RECIDENTE ESPECIAL'
        WHEN a.TIPDOCUM = 15 THEN 'PERMISO PROTECCION TEMPORAL'
        ELSE 'OTRO'
        END AS TIPO_DOCUMENTO,				 
        a.POBLACION_ESPECIAL,
        a.APELLIDO1, 
        a.APELLIDO2, 
        a.NOMBRE1,
        a.NOMBRE2,
        CONVERT(date, a.FECHANAC, 120) AS FECHA_NACIMIENTO, 
        CASE
        WHEN a.SEXO = 2 THEN 'FEMENINO'
        WHEN a.SEXO = 1 THEN 'MASCULINO' 
        ELSE 'OTRO'
        END AS GENERO,
        CASE
        WHEN e.EMBARAZO = 0 THEN 'NO'
        WHEN e.EMBARAZO = 1 THEN 'EMBARAZADA' 
        END AS EMBARAZO,
        CASE
        WHEN a.TIPDISCAP = 1 THEN 'CONDUCTA'
        WHEN a.TIPDISCAP = 2 THEN 'COMUNICACION'
        WHEN a.TIPDISCAP = 3 THEN 'CUIDADO PERSONAL'
        WHEN a.TIPDISCAP = 4 THEN 'LOCOMOCION'
        WHEN a.TIPDISCAP = 5 THEN 'DISPOSICION DEL CUERPO'
        WHEN a.TIPDISCAP = 6 THEN 'DESTREZA'
        WHEN a.TIPDISCAP = 7 THEN 'SITUACION'
        WHEN a.TIPDISCAP = 8 THEN 'DETERMINADA ACTITUD'
        WHEN a.TIPDISCAP = 9 THEN 'OTRO'
        END AS TIPO_DISCAPACIDAD,
        CASE
        WHEN a.GRDDISCAP = 1 THEN 'LEVE'
        WHEN a.GRDDISCAP = 2 THEN 'MODERADO' 
        WHEN a.GRDDISCAP = 3 THEN 'SEVERA'
        END AS GRADO_DISCAPACIDAD,
        a.DIRECRES AS DIRECCION_RECIDENCIA, 
        a.TELEFRES AS TELEFONO_RECIDENCIA,
        a.CODBARES AS CODIGO_BARRIO,
        d.NOMBRE AS COMUNA,
        c.NOMBRE AS BARRIO,
        CASE
        WHEN a.ETNICO = 1 THEN 'BLANCO'
        WHEN a.ETNICO = 2 THEN 'INDIGENA'
        WHEN a.ETNICO = 3 THEN 'AFRODECENDIENTE'
        WHEN a.ETNICO = 4 THEN 'MESTIZO(IND-BLA)'
        WHEN a.ETNICO = 5 THEN 'MULATO(NEG-BLA)'
        WHEN a.ETNICO = 6 THEN 'ZAMBO(IND-NEG)'
        WHEN a.ETNICO = 7 THEN 'GITANO(ROM)'
        WHEN a.ETNICO = 8 THEN 'RAIZAL(SAN ANDRES)'
        WHEN a.ETNICO = 9 THEN 'PALENQUERO'
        ELSE 'OTRO'
        END AS ETNICO,
        b.NOMBRE AS PAIS, 
        h.NOMBRE, 
        f.CODIGO, 
        CASE
        WHEN f.ATENDIDO = 0 THEN 'NO'
        WHEN f.ATENDIDO = 1 THEN 'SI'
        END AS ATENDIDO,
        f.ATENCION_FACTURA,
        DATEPART(DAY, a.FECHANAC) AS DIA,
        DATEPART(MONTH, a.FECHANAC) AS MES,
        DATEPART(YEAR, a.FECHANAC) AS AÑO,
        CONCAT(
        CAST(
            FLOOR(
            (CAST(CONVERT(VARCHAR(8), GETDATE(), 112) AS INT) -
            CAST(CONVERT(VARCHAR(8), a.FECHANAC, 112) AS INT)) / 10000
            ) AS VARCHAR(10)
            ),
            ' ',
            CASE e.FORMEDAD
            WHEN 1 THEN 'AÑOS'
            WHEN 2 THEN 'MESES'
            WHEN 3 THEN 'DIAS'
            WHEN 4 THEN 'HORAS'
            END) AS EDAD,
        MAX(CASE WHEN j.CODIGO = '890105' THEN 'ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA' END) AS '890105',
        MAX(CASE WHEN j.CODIGO = '890114' THEN 'ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD' END) AS '890114',
        MAX(CASE WHEN j.CODIGO = '890115' THEN 'ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO' END) AS '890115',
        MAX(CASE WHEN j.CODIGO = '890116' THEN 'ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD' END) AS '890116',
        MAX(CASE WHEN j.CODIGO = '890201' THEN 'ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO' END) AS '890201',
        MAX(CASE WHEN j.CODIGO = '890203' THEN 'CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL' END) AS '890203',
        MAX(CASE WHEN j.CODIGO = '890205' THEN 'CONSULTA DE PRIMERA VEZ POR ENFERMERIA' END) AS '890205',
        MAX(CASE WHEN j.CODIGO = '890206' THEN 'CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA' END) AS '890206',
        MAX(CASE WHEN j.CODIGO = '890263' THEN 'CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR' END) AS '890263',
        MAX(CASE WHEN j.CODIGO = '890283' THEN 'CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN PEDIATRÍA' END) AS '890283',
        MAX(CASE WHEN j.CODIGO = '890301' THEN 'CONSULTA DE CONTROL O DE SEGUIMIENTO POR MEDICINA GENERAL' END) AS '890301',
        MAX(CASE WHEN j.CODIGO = '890303' THEN 'CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL' END) AS '890303',
        MAX(CASE WHEN j.CODIGO = '890305' THEN 'CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA' END) AS '890305',
        MAX(CASE WHEN j.CODIGO = '890306' THEN 'CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA' END) AS '890306',
        MAX(CASE WHEN j.CODIGO = '890363' THEN 'CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR' END) AS '890363',
        MAX(CASE WHEN j.CODIGO = '890383' THEN 'CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN PEDIATRÍA' END) AS '890383',
        MAX(CASE WHEN j.CODIGO = '902213' THEN 'HEMOGLOBINA' END) AS '902213',
        MAX(CASE WHEN j.CODIGO = '950601' THEN 'MEDICION DE AGUDEZA VISUAL' END) AS '950601',
        MAX(CASE WHEN j.CODIGO = '990101' THEN 'EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL' END) AS '990101',
        MAX(CASE WHEN j.CODIGO = '990102' THEN 'EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA' END) AS '990102',
        MAX(CASE WHEN j.CODIGO = '990103' THEN 'EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA' END) AS '990103',
        MAX(CASE WHEN j.CODIGO = '990104' THEN 'EDUCACION GRUPAL EN SALUD, POR ENFERMERIA' END) AS '990104',
        MAX(CASE WHEN j.CODIGO = '990105' THEN 'EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA' END) AS '990105',
        MAX(CASE WHEN j.CODIGO = '990106' THEN 'EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA' END) AS '990106',
        MAX(CASE WHEN j.CODIGO = '990107' THEN 'EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL' END) AS '990107',
        MAX(CASE WHEN j.CODIGO = '990108' THEN 'EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA' END) AS '990108',
        MAX(CASE WHEN j.CODIGO = '990109' THEN 'EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL' END) AS '990109',
        MAX(CASE WHEN j.CODIGO = '990110' THEN 'EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA' END) AS '990110',
        MAX(CASE WHEN j.CODIGO = '990111' THEN 'EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO' END) AS '990111',
        MAX(CASE WHEN j.CODIGO = '990112' THEN 'EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL' END) AS '990112',
        MAX(CASE WHEN j.CODIGO = '990201' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL' END) AS '990201',
        MAX(CASE WHEN j.CODIGO = '990203' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA' END) AS '990203',
        MAX(CASE WHEN j.CODIGO = '990204' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA' END) AS '990204',
        MAX(CASE WHEN j.CODIGO = '990206' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA' END) AS '990206',
        MAX(CASE WHEN j.CODIGO = '990207' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL' END) AS '990207',
        MAX(CASE WHEN j.CODIGO = '990208' THEN 'EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA' END) AS '990208',
        MAX(CASE WHEN j.CODIGO = '990209' THEN 'EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL' END) AS '990209',
        MAX(CASE WHEN j.CODIGO = '990212' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL' END) AS '990212',
        MAX(CASE WHEN j.CODIGO = '990213' THEN 'EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO' END) AS '990213',
        MAX(CASE WHEN j.CODIGO = '990221' THEN 'EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN INFANTIL Y ADOLESCENTE' END) AS '990221',
        MAX(CASE WHEN j.CODIGO = '990222' THEN 'EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL' END) AS '990222',
        MAX(CASE WHEN j.CODIGO = '990223' THEN 'EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES' END) AS '990223',
        MAX(CASE WHEN j.CODIGO = '990224' THEN 'EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR' END) AS '990224',
        MAX(CASE WHEN j.CODIGO = '993102' THEN 'VACUNACION CONTRA TUBERCULOSIS [BCG]' END) AS '993102',
        MAX(CASE WHEN j.CODIGO = '993106' THEN 'VACUNACIÓN CONTRA NEUMOCOCO' END) AS '993106',
        MAX(CASE WHEN j.CODIGO = '993122' THEN 'VACUNACIÓN COMBINADA CONTRA DIFTERIA, TÉTANOS Y TOS FERINA [DPT]' END) AS '993122',
        MAX(CASE WHEN j.CODIGO = '993130' THEN 'VACUNACIÓN COMBINADA CONTRA HAEMOPHILUS INFLUENZA TIPO B, DIFTERIA, TÉTANOS, TOS FERINA Y HEPATITIS B (PENTAVALENTE)' END) AS '993130',
        MAX(CASE WHEN j.CODIGO = '993501' THEN 'VACUNACION CONTRA POLIOMIELITIS (VOP O IVP)' END) AS '993501',
        MAX(CASE WHEN j.CODIGO = '99350101' THEN 'VACUNACIÓN CONTRA POLIOMIELITIS (VOP O IVP) INTRAMUSCULAR' END) AS '99350101',
        MAX(CASE WHEN j.CODIGO = '993502' THEN 'VACUNACION CONTRA HEPATITIS A' END) AS '993502',
        MAX(CASE WHEN j.CODIGO = '993503' THEN 'VACUNACION CONTRA Hepatitis B' END) AS '993503',
        MAX(CASE WHEN j.CODIGO = '993504' THEN 'VACUNACION CONTRA FIEBRE AMARILLA' END) AS '993504',
        MAX(CASE WHEN j.CODIGO = '993509' THEN 'VACUNACION CONTRA VARICELA' END) AS '993509',
        MAX(CASE WHEN j.CODIGO = '99351003' THEN 'VACUNA CONTRA INFLUENZA PEDIATRICA  (H1N1)' END) AS '99351003',
        MAX(CASE WHEN j.CODIGO = '993512' THEN 'VACUNACION CONTRA ROTAVIRUS' END) AS '993512',
        MAX(CASE WHEN j.CODIGO = '993522' THEN 'VACUNACIÓN COMBINADA CONTRA SARAMPIÓN, PAROTIDITIS Y RUBÉOLA [SRP] (TRIPLE VIRAL)' END) AS '993522',
        MAX(CASE WHEN j.CODIGO = '997102' THEN 'APLICACION DE SELLANTES DE FOTOCURADO' END) AS '997102',
        MAX(CASE WHEN j.CODIGO = '997106' THEN 'TOPICACIÓN DE FLUOR EN BARNIZ' END) AS '997106',
        MAX(CASE WHEN j.CODIGO = '997310' THEN 'CONTROL DE PLACA DENTAL -' END) AS '997310',
        MAX(CASE WHEN j.CODIGO = 'P0000002' THEN 'CONSEJERIA EN VIH' END) AS 'P0000002',
        MAX(CASE WHEN j.CODIGO = 'P0000004' THEN 'DEMANDA INDUCIDA CANALIZADOS ALTERACIONES DE LA AGUDEZA VISUAL' END) AS 'P0000004',
        MAX(CASE WHEN j.CODIGO = 'P0000006' THEN 'DEMANDA INDUCIDA CANALIZADOS ATENCION DEL PARTO' END) AS 'P0000006',
        MAX(CASE WHEN j.CODIGO = 'P0000009' THEN 'DEMANDA INDUCIDA CANALIZADOS CRECIMIENTO Y DESARROLLO' END) AS 'P0000009',
        MAX(CASE WHEN j.CODIGO = 'P0000013' THEN 'DEMANDA INDUCIDA CANALIZADOS VACUNACION PAI' END) AS 'P0000013',
        MAX(CASE WHEN j.CODIGO = 'P0000014' THEN 'DEMANDA INDUCIDA CANALIZADOS VALORACION DEL JOVEN' END) AS 'P0000014'
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
            ) BETWEEN ${edadInicial} AND ${edadFinal}  AND f.ATENCION_FACTURA  IS NOT NULL
            GROUP BY j.CODIGO, j.NOMBRE,a.NUMDOCUM,f.IPS,g.NOMBRE,a.TIPDOCUM,a.POBLACION_ESPECIAL,a.APELLIDO1, 
            a.APELLIDO2,a.NOMBRE1,a.NOMBRE2,a.FECHANAC,a.SEXO,e.EMBARAZO,a.TIPDISCAP,a.GRDDISCAP,a.DIRECRES,
            a.TELEFRES,a.CODBARES,d.NOMBRE,c.NOMBRE,a.ETNICO,b.NOMBRE,h.NOMBRE,j.NOMBRE,f.CODIGO, f.ATENDIDO,f.ATENCION_FACTURA,
            e.FORMEDAD
            ORDER BY a.NUMDOCUM ASC`;
        const resul = await sequelize.query(query, {type: QueryTypes.SELECT}); 
      /* console.log(Object.keys(resul[0])); */
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Datos');
        const columnas = Object.keys(resul[0]);

        const headerFont = {
            name: 'Arial',
            size: 10,
            bold: true
        };
        
        const headerFill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '87CEEB' }
        };
        
          // Separar las columnas en dos grupos
        const stringColumns = [];
        const numericColumns = [];
        
        columnas.forEach((column) => {
        if (isNaN(column)) {
            stringColumns.push(column);
            } else {
            numericColumns.push(column);
            }
        });
        
        // Ordenar cada grupo por separado
        stringColumns.sort();
        numericColumns.sort();
        
        // Combinar los dos grupos en un solo array
        const orderedColumns = stringColumns.concat(numericColumns);
        
        // Configurar las columnas en el archivo de Excel utilizando el nuevo orden
        worksheet.columns = orderedColumns.map((column) => {
        return { header: column, key: column, width: 40 };
        });
        
        // Establecer estilos de las celdas
        const cellStyle = {
            alignment: { horizontal: 'center' },
            border: {
            top: { style: 'thin' },
            left: { style: 'thin' },
            bottom: { style: 'thin' },
            right: { style: 'thin' }
            }
        };
        
        worksheet.eachRow({ includeEmpty: true }, (row) => {
            row.eachCell({ includeEmpty: true }, (cell) => {
            cell.style = cellStyle;
            });
        });
        
        // Agregar los encabezados
        const headerRow = worksheet.getRow(1);
        headerRow.eachCell((cell) => {
            cell.alignment = { vertical: 'middle', horizontal: 'center' };
            cell.fill = headerFill;
            cell.font = headerFont;
        });
        
        // Agregar los datos
        resul.forEach((item) => {
            const rowData = [];
            orderedColumns.forEach((column) => {
            rowData.push(item[column]);
            });
            worksheet.addRow(rowData);
        });
        // Establecer las cabeceras de respuesta para descargar el archivo
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            'attachment; filename=datos.xlsx'
        );
        // Escribir el archivo de Excel en la respuesta
            workbook.xlsx.write(res)
            .then(() => {
                res.end();
            })
        }  
            
        catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
        
        }

}
