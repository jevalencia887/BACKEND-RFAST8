const { QueryTypes, and } = require('sequelize');
const {sequelize} = require('../BD-RFAS8/conexiondb');


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
		WHEN e.EMBARAZO = 1 THEN 'SI' 
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
        f.ATENCION_FACTURA, 
        f.CODIGO, 
        f.ATENDIDO,
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
		WHEN e.EMBARAZO = 1 THEN 'SI' 
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
        f.ATENCION_FACTURA, 
        f.CODIGO, 
        f.ATENDIDO,
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
        //console.log(atendidos);

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
        //console.log(noAtendidos);
        
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
