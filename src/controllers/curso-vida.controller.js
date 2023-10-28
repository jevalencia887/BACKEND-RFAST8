const { QueryTypes, and } = require('sequelize');
const {sequelize} = require('../BD-RFAS8/conexiondb');
const ExcelJS = require('exceljs');



exports.cursoVida = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO; 
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const query = `
        DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
        DECLARE @FechaFinal DATE = GETDATE();

        CREATE TABLE #TempResults (
            DUPLICADOS NVARCHAR(255), 
            IPS NVARCHAR(255), 
            NOMBRE_IPS NVARCHAR(255),
            TIPO_DOCUMENTO NVARCHAR(255),
            POBLACION_ESPECIAL INT, 
            APELLIDOS NVARCHAR(255),
            NOMBRES NVARCHAR(255),
            FECHA_NACIMIENTO DATE,  
            GENERO NVARCHAR(50), 
            EMBARAZO NVARCHAR(10), 
            TIPO_DISCAPACIDAD NVARCHAR(50), 
            GRADO_DISCAPACIDAD NVARCHAR(50), 
            DIRECCION_RESIDENCIA NVARCHAR(255), 
            TELEFONO_RESIDENCIA NVARCHAR(20), 
            CODIGO_BARRIO NVARCHAR(255),  
            COMUNA NVARCHAR(255), 
            BARRIO NVARCHAR(255), 
            ETNICO NVARCHAR(50), 
            PAIS NVARCHAR(255),  
            CENTRO_PRODUCCION NVARCHAR(255), 
            CODIGO NVARCHAR(255),  
            ATENDIDO NVARCHAR(5), 
            ATENCION_FACTURA DATE, 
            ESTADO NVARCHAR(50), 
            DIA NVARCHAR(5),  
            MES NVARCHAR(5),  
            AÑO NVARCHAR(5),  
            EDAD NVARCHAR(50),  
            NUMDOCUM NVARCHAR(255),
            DescripcionServicio NVARCHAR(255),
            NOMBRE_SERVICIO NVARCHAR(255)
        );
        
        INSERT INTO #TempResults
        SELECT
            ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM, f.IPS, g.NOMBRE, a.TIPDOCUM, a.POBLACION_ESPECIAL, a.APELLIDO1,
                                            a.NOMBRE1, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                                            a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                                            a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO
            ORDER BY a.NUMDOCUM) AS DUPLICADO,
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
            CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
            CONVERT(varchar, CONVERT(date, ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
            j.NOMBRE AS NOMBRE_SERVICIO
            FROM fac_m_tarjetero a 
            INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
            INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
            INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
            INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
            INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
            INNER JOIN fac_p_control g ON g.IPS = f.IPS 
            INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD  
            INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS 
            WHERE h.CODIGO = ${CODIGO}
            AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
            AND f.ATENCION_FACTURA IS NOT NULL
            AND f.CODIGO_CUPS IS NOT NULL
            AND f.ATENDIDO = 1
            AND f.ESTADO = 1
            AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
        
        ;WITH Duplicates AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADOS) AS RowNum
            FROM #TempResults
        )
        DELETE FROM Duplicates WHERE RowNum > 1;
        
        SELECT *
        FROM #TempResults;

        DROP TABLE #TempResults;`;
        const resul = await sequelize.query(query, {type: QueryTypes.SELECT});
        
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
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

        const arrayId = req.body;
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
        
        const query1 = `

        DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
        DECLARE @FechaFinal DATE = GETDATE();

            CREATE TABLE #TempResults (
            DUPLICADOS NVARCHAR(255), 
            IPS NVARCHAR(255), 
            NOMBRE_IPS NVARCHAR(255),
            TIPO_DOCUMENTO NVARCHAR(255),
            POBLACION_ESPECIAL INT, 
            APELLIDOS NVARCHAR(255),
            NOMBRES NVARCHAR(255),
            FECHA_NACIMIENTO DATE,  
            GENERO NVARCHAR(50), 
            EMBARAZO NVARCHAR(10), 
            TIPO_DISCAPACIDAD NVARCHAR(50), 
            GRADO_DISCAPACIDAD NVARCHAR(50), 
            DIRECCION_RESIDENCIA NVARCHAR(255), 
            TELEFONO_RESIDENCIA NVARCHAR(20), 
            CODIGO_BARRIO NVARCHAR(255),  
            COMUNA NVARCHAR(255), 
            BARRIO NVARCHAR(255), 
            ETNICO NVARCHAR(50), 
            PAIS NVARCHAR(255),  
            CENTRO_PRODUCCION NVARCHAR(255), 
            CODIGO NVARCHAR(255),  
            ATENDIDO NVARCHAR(5), 
            ATENCION_FACTURA DATE, 
            ESTADO NVARCHAR(50), 
            DIA NVARCHAR(5),  
            MES NVARCHAR(5),  
            AÑO NVARCHAR(5),  
            EDAD NVARCHAR(50),  
            NUMDOCUM NVARCHAR(255),
            DescripcionServicio NVARCHAR(255),
            NOMBRE_SERVICIO NVARCHAR(255)
        );
        
        INSERT INTO #TempResults
        SELECT
            ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM, f.IPS, g.NOMBRE, a.TIPDOCUM, a.POBLACION_ESPECIAL, a.APELLIDO1,
                                            a.NOMBRE1, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                                            a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                                            a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO
            ORDER BY a.NUMDOCUM) AS DUPLICADO,
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
            CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
            CONVERT(varchar, CONVERT(date, ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
            j.NOMBRE AS NOMBRE_SERVICIO
            FROM fac_m_tarjetero a 
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
            AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal

            ${it != "" ? 'AND ('+ it.substring(3, it.length)+')' : ''}
        
        ;WITH Duplicates AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADOS) AS RowNum
            FROM #TempResults
        )
        DELETE FROM Duplicates WHERE RowNum > 1;
        
        SELECT *
        FROM #TempResults;

        DROP TABLE #TempResults;`

        const resul = await sequelize.query(query1, {type: QueryTypes.SELECT});
        /* console.log(resul); */
        
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            data: resul
        })
        
    } catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
    }
}


// Función para ejecutar la consulta SQL y obtener los resultados
async function executeSQLQuery0(CODIGO, edadInicial, edadFinal) {
    const query = `
    DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
    DECLARE @FechaFinal DATE = GETDATE();

    CREATE TABLE #TempResultsPivoted (
    DUPLICADO NVARCHAR(255), 
    NOMBRE_IPS NVARCHAR(255),
	COD_EPS NVARCHAR(255),
	NOM_ASEGURADORA NVARCHAR(255),
    TIPO_DOCUMENTO NVARCHAR(255),
    POBLACION_ESPECIAL NVARCHAR(255), 
    PRIMER_APELLIDO NVARCHAR(255),
	SEGUNDO_APELLIDO NVARCHAR(255),
    PRIMER_NOMBRE NVARCHAR(255),
	SEGUNDO_NOMBRE NVARCHAR(255),
    FECHA_NACIMIENTO DATE,  
    GENERO NVARCHAR(50), 
    EMBARAZO NVARCHAR(10), 
    TIPO_DISCAPACIDAD NVARCHAR(50), 
    GRADO_DISCAPACIDAD NVARCHAR(50), 
    DIRECCION_RESIDENCIA NVARCHAR(255), 
    TELEFONO_RESIDENCIA NVARCHAR(20), 
    CODIGO_BARRIO NVARCHAR(255),  
    COMUNA NVARCHAR(255), 
    BARRIO NVARCHAR(255), 
    ETNICO NVARCHAR(50), 
    PAIS NVARCHAR(255),  
    CENTRO_PRODUCCION NVARCHAR(255), 
    CODIGO NVARCHAR(255),  
    ATENDIDO NVARCHAR(5), 
    ATENCION_FACTURA DATE, 
    ESTADO NVARCHAR(50), 
    DIA NVARCHAR(5),  
    MES NVARCHAR(5),  
    AÑO NVARCHAR(5),  
    EDAD NVARCHAR(50),
	DIAGNOSTICO NVARCHAR(255),
	CAUSA NVARCHAR(255),
    NUMDOCUM NVARCHAR(255),
    DescripcionServicio NVARCHAR(255),
    NOMBRE_SERVICIO NVARCHAR(255)
);

    INSERT INTO #TempResultsPivoted
    SELECT
    ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM,  g.NOMBRE, ñ.EMPRESA, o.NOMBRE, a.TIPDOCUM,  q.POBLACION_ESPECIAL, a.APELLIDO1, a.APELLIDO2,
                        a.NOMBRE1, a.NOMBRE2, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                        a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                        a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO, m.NOMBRE, n.NOMBRE
	ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
    g.NOMBRE AS NOMBRE_IPS,
	ñ.EMPRESA AS COD_EPS,
	o.NOMBRE AS NOM_ASEGURADORA,
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
    CASE q.POBLACION_ESPECIAL
    WHEN 1 THEN 'Personas de la tercera edad en protección de ancianatos'
    WHEN 2 THEN 'Indigenas mayor de edad'
    WHEN 3 THEN 'Habitante de la calle mayor de edad'
    WHEN 4 THEN 'Habitante de la calle menor de edad'
    WHEN 5 THEN 'Menor de edad desvinculado del conflicto armado'
    WHEN 6 THEN 'Población infantil vulnerable en instituciones diferentes al ICBF'
    WHEN 7 THEN 'Población infantil vulnerable a cargo del ICBF'
    WHEN 8 THEN 'Indigena menor de edad'
    WHEN 9 THEN 'Recien nacido con edad menor o igual a 30 días'
    WHEN 10 THEN 'Menor de edad desplazado'
    WHEN 11 THEN 'Mayor de edad desplazado'
    WHEN 12 THEN 'Recluso menor de edad'
    WHEN 13 THEN 'Recluso mayor de edad'
    WHEN 99 THEN 'Ninguno'
    WHEN 98 THEN 'Extranjero en tránsito'
    END AS POBLACION_ESPECIAL,  
    a.APELLIDO1 AS PRIMER_APELLIDO,
	a.APELLIDO2 AS SEGUNDO_APELLIDO,
    a.NOMBRE1 AS PRIMER_NOMBRE,
	a.NOMBRE2 AS SEGUNDO_NOMBRE,
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
    CONCAT(e.CANTEDAD, ' ' +
    CASE e.FORMEDAD 
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    m.NOMBRE AS DIAGNOSTICO,
    n.NOMBRE AS CAUSA,
    CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
    CONVERT(varchar, CONVERT(DATE, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
    j.NOMBRE AS NOMBRE_SERVICIO
    FROM fac_m_tarjetero a 
    INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
    INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
    INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
    INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
    INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
    INNER JOIN fac_p_control g ON g.IPS = f.IPS 
    INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    INNER JOIN fac_m_procedimientos i ON i.IPS = e.IPS 
    AND i.DOCUMENTO = e.DOCUMENTO AND i.FACTURA = e.FACTURA
    INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    INNER JOIN fac_m_procedimientos_dx k ON k.IPS = i.IPS AND k.DOCUMENTO = i.DOCUMENTO 
    AND k.FACTURA = i.FACTURA AND K.CODIGO = I.CODIGO AND k.ATENCION = i.ATENCION
    INNER JOIN fac_p_diagnostico m ON m.DIAGNOSTICO = k.DIAGNOSTICO
    INNER JOIN fac_p_causa n ON n.CODIGO = k.CAUSA
    INNER JOIN fac_m_usuarios890 ñ ON ñ.HISTORIA = a.HISTORIA
    INNER JOIN gen_p_eps o ON o.CODIGO = ñ.EMPRESA
    INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
    AND f.ESTADO = 1
    AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
    
    ;WITH Duplicates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADO) AS RowNum
        FROM #TempResultsPivoted
    )
    DELETE FROM Duplicates WHERE RowNum > 1;

    DECLARE @ServiceNames NVARCHAR(MAX);
    SELECT @ServiceNames = COALESCE(@ServiceNames + ', ', '') + QUOTENAME(NOMBRE_SERVICIO)
    FROM #TempResultsPivoted
    GROUP BY NOMBRE_SERVICIO;

    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT *
    FROM (
        SELECT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, PRIMER_APELLIDO, SEGUNDO_APELLIDO, 
				PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, 
				GRADO_DISCAPACIDAD, DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
				BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
				MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
    MAX(DescripcionServicio) 
    FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';

    EXEC sp_executesql @SqlQuery;

    DROP TABLE #TempResultsPivoted`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results, sheetName) {
    const workbook = new ExcelJS.Workbook();
    
    // Agregar una hoja de Excel y establecer su nombre
    const worksheet = workbook.addWorksheet(sheetName);

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
exports.cursoVidaExcel0 = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const sheetName = 'PRIMERA INFANCIA'; // Personaliza el nombre de la hoja aquí
        const excelTitle = 'CursoVida-PrimeraInfancia'; // Personaliza el título del libro de Excel aquí

        // Ejecutar la consulta SQL y obtener los resultados
        const results = await executeSQLQuery0(CODIGO, edadInicial, edadFinal);

        // Llama a la función configureExcelWorkbook con los resultados y el nombre de la hoja como parámetros
        const workbook = configureExcelWorkbook(results, sheetName);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${excelTitle}.xlsx`
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
async function executeSQLQuery1(CODIGO, edadInicial, edadFinal) {
    const query = `
    DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
    DECLARE @FechaFinal DATE = GETDATE();

    CREATE TABLE #TempResultsPivoted (
    DUPLICADO NVARCHAR(255), 
    NOMBRE_IPS NVARCHAR(255),
	COD_EPS NVARCHAR(255),
	NOM_ASEGURADORA NVARCHAR(255),
    TIPO_DOCUMENTO NVARCHAR(255),
    POBLACION_ESPECIAL NVARCHAR(255), 
    PRIMER_APELLIDO NVARCHAR(255),
	SEGUNDO_APELLIDO NVARCHAR(255),
    PRIMER_NOMBRE NVARCHAR(255),
	SEGUNDO_NOMBRE NVARCHAR(255),
    FECHA_NACIMIENTO DATE,  
    GENERO NVARCHAR(50), 
    EMBARAZO NVARCHAR(10), 
    TIPO_DISCAPACIDAD NVARCHAR(50), 
    GRADO_DISCAPACIDAD NVARCHAR(50), 
    DIRECCION_RESIDENCIA NVARCHAR(255), 
    TELEFONO_RESIDENCIA NVARCHAR(20), 
    CODIGO_BARRIO NVARCHAR(255),  
    COMUNA NVARCHAR(255), 
    BARRIO NVARCHAR(255), 
    ETNICO NVARCHAR(50), 
    PAIS NVARCHAR(255),  
    CENTRO_PRODUCCION NVARCHAR(255), 
    CODIGO NVARCHAR(255),  
    ATENDIDO NVARCHAR(5), 
    ATENCION_FACTURA DATE, 
    ESTADO NVARCHAR(50), 
    DIA NVARCHAR(5),  
    MES NVARCHAR(5),  
    AÑO NVARCHAR(5),  
    EDAD NVARCHAR(50),
	DIAGNOSTICO NVARCHAR(255),
	CAUSA NVARCHAR(255),
    NUMDOCUM NVARCHAR(255),
    DescripcionServicio NVARCHAR(255),
    NOMBRE_SERVICIO NVARCHAR(255)
);

    INSERT INTO #TempResultsPivoted
    SELECT
    ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM,  g.NOMBRE, ñ.EMPRESA, o.NOMBRE, a.TIPDOCUM,  q.POBLACION_ESPECIAL, a.APELLIDO1, a.APELLIDO2,
                        a.NOMBRE1, a.NOMBRE2, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                        a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                        a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO, m.NOMBRE, n.NOMBRE
	ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
    g.NOMBRE AS NOMBRE_IPS,
	ñ.EMPRESA AS COD_EPS,
	o.NOMBRE AS NOM_ASEGURADORA,
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
    CASE q.POBLACION_ESPECIAL
    WHEN 1 THEN 'Personas de la tercera edad en protección de ancianatos'
    WHEN 2 THEN 'Indigenas mayor de edad'
    WHEN 3 THEN 'Habitante de la calle mayor de edad'
    WHEN 4 THEN 'Habitante de la calle menor de edad'
    WHEN 5 THEN 'Menor de edad desvinculado del conflicto armado'
    WHEN 6 THEN 'Población infantil vulnerable en instituciones diferentes al ICBF'
    WHEN 7 THEN 'Población infantil vulnerable a cargo del ICBF'
    WHEN 8 THEN 'Indigena menor de edad'
    WHEN 9 THEN 'Recien nacido con edad menor o igual a 30 días'
    WHEN 10 THEN 'Menor de edad desplazado'
    WHEN 11 THEN 'Mayor de edad desplazado'
    WHEN 12 THEN 'Recluso menor de edad'
    WHEN 13 THEN 'Recluso mayor de edad'
    WHEN 99 THEN 'Ninguno'
    WHEN 98 THEN 'Extranjero en tránsito'
    END AS POBLACION_ESPECIAL,  
    a.APELLIDO1 AS PRIMER_APELLIDO,
	a.APELLIDO2 AS SEGUNDO_APELLIDO,
    a.NOMBRE1 AS PRIMER_NOMBRE,
	a.NOMBRE2 AS SEGUNDO_NOMBRE,
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
    CONCAT(e.CANTEDAD, ' ' +
    CASE e.FORMEDAD 
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    m.NOMBRE AS DIAGNOSTICO,
    n.NOMBRE AS CAUSA,
    CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
    CONVERT(varchar, CONVERT(DATE, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
    j.NOMBRE AS NOMBRE_SERVICIO
    FROM fac_m_tarjetero a 
    INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
    INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
    INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
    INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
    INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
    INNER JOIN fac_p_control g ON g.IPS = f.IPS 
    INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    INNER JOIN fac_m_procedimientos i ON i.IPS = e.IPS 
    AND i.DOCUMENTO = e.DOCUMENTO AND i.FACTURA = e.FACTURA
    INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    INNER JOIN fac_m_procedimientos_dx k ON k.IPS = i.IPS AND k.DOCUMENTO = i.DOCUMENTO 
    AND k.FACTURA = i.FACTURA AND K.CODIGO = I.CODIGO AND k.ATENCION = i.ATENCION
    INNER JOIN fac_p_diagnostico m ON m.DIAGNOSTICO = k.DIAGNOSTICO
    INNER JOIN fac_p_causa n ON n.CODIGO = k.CAUSA
    INNER JOIN fac_m_usuarios890 ñ ON ñ.HISTORIA = a.HISTORIA
    INNER JOIN gen_p_eps o ON o.CODIGO = ñ.EMPRESA
    INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
    AND f.ESTADO = 1
    AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
    
    ;WITH Duplicates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADO) AS RowNum
        FROM #TempResultsPivoted
    )
    DELETE FROM Duplicates WHERE RowNum > 1;

    DECLARE @ServiceNames NVARCHAR(MAX);
    SELECT @ServiceNames = COALESCE(@ServiceNames + ', ', '') + QUOTENAME(NOMBRE_SERVICIO)
    FROM #TempResultsPivoted
    GROUP BY NOMBRE_SERVICIO;

    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT *
    FROM (
        SELECT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, PRIMER_APELLIDO, SEGUNDO_APELLIDO, 
				PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, 
				GRADO_DISCAPACIDAD, DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
				BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
				MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
    MAX(DescripcionServicio) 
    FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';

    EXEC sp_executesql @SqlQuery;

    DROP TABLE #TempResultsPivoted`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results, sheetName) {
    const workbook = new ExcelJS.Workbook();
    
    // Agregar una hoja de Excel y establecer su nombre
    const worksheet = workbook.addWorksheet(sheetName);

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
        const sheetName = 'INFANCIA'; // Personaliza el nombre de la hoja aquí
        const excelTitle = 'CursoVida-Infancia'; // Personaliza el título del libro de Excel aquí

        // Ejecutar la consulta SQL y obtener los resultados
        const results = await executeSQLQuery1(CODIGO, edadInicial, edadFinal);

        // Llama a la función configureExcelWorkbook con los resultados y el nombre de la hoja como parámetros
        const workbook = configureExcelWorkbook(results, sheetName);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${excelTitle}.xlsx`
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
async function executeSQLQuery2(CODIGO, edadInicial, edadFinal) {
    const query = `
    DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
    DECLARE @FechaFinal DATE = GETDATE();

    CREATE TABLE #TempResultsPivoted (
    DUPLICADO NVARCHAR(255), 
    NOMBRE_IPS NVARCHAR(255),
	COD_EPS NVARCHAR(255),
	NOM_ASEGURADORA NVARCHAR(255),
    TIPO_DOCUMENTO NVARCHAR(255),
    POBLACION_ESPECIAL NVARCHAR(255), 
    PRIMER_APELLIDO NVARCHAR(255),
	SEGUNDO_APELLIDO NVARCHAR(255),
    PRIMER_NOMBRE NVARCHAR(255),
	SEGUNDO_NOMBRE NVARCHAR(255),
    FECHA_NACIMIENTO DATE,  
    GENERO NVARCHAR(50), 
    EMBARAZO NVARCHAR(10), 
    TIPO_DISCAPACIDAD NVARCHAR(50), 
    GRADO_DISCAPACIDAD NVARCHAR(50), 
    DIRECCION_RESIDENCIA NVARCHAR(255), 
    TELEFONO_RESIDENCIA NVARCHAR(20), 
    CODIGO_BARRIO NVARCHAR(255),  
    COMUNA NVARCHAR(255), 
    BARRIO NVARCHAR(255), 
    ETNICO NVARCHAR(50), 
    PAIS NVARCHAR(255),  
    CENTRO_PRODUCCION NVARCHAR(255), 
    CODIGO NVARCHAR(255),  
    ATENDIDO NVARCHAR(5), 
    ATENCION_FACTURA DATE, 
    ESTADO NVARCHAR(50), 
    DIA NVARCHAR(5),  
    MES NVARCHAR(5),  
    AÑO NVARCHAR(5),  
    EDAD NVARCHAR(50),
	DIAGNOSTICO NVARCHAR(255),
	CAUSA NVARCHAR(255),
    NUMDOCUM NVARCHAR(255),
    DescripcionServicio NVARCHAR(255),
    NOMBRE_SERVICIO NVARCHAR(255)
);

    INSERT INTO #TempResultsPivoted
    SELECT
    ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM,  g.NOMBRE, ñ.EMPRESA, o.NOMBRE, a.TIPDOCUM,  q.POBLACION_ESPECIAL, a.APELLIDO1, a.APELLIDO2,
                        a.NOMBRE1, a.NOMBRE2, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                        a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                        a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO, m.NOMBRE, n.NOMBRE
	ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
    g.NOMBRE AS NOMBRE_IPS,
	ñ.EMPRESA AS COD_EPS,
	o.NOMBRE AS NOM_ASEGURADORA,
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
    CASE q.POBLACION_ESPECIAL
    WHEN 1 THEN 'Personas de la tercera edad en protección de ancianatos'
    WHEN 2 THEN 'Indigenas mayor de edad'
    WHEN 3 THEN 'Habitante de la calle mayor de edad'
    WHEN 4 THEN 'Habitante de la calle menor de edad'
    WHEN 5 THEN 'Menor de edad desvinculado del conflicto armado'
    WHEN 6 THEN 'Población infantil vulnerable en instituciones diferentes al ICBF'
    WHEN 7 THEN 'Población infantil vulnerable a cargo del ICBF'
    WHEN 8 THEN 'Indigena menor de edad'
    WHEN 9 THEN 'Recien nacido con edad menor o igual a 30 días'
    WHEN 10 THEN 'Menor de edad desplazado'
    WHEN 11 THEN 'Mayor de edad desplazado'
    WHEN 12 THEN 'Recluso menor de edad'
    WHEN 13 THEN 'Recluso mayor de edad'
    WHEN 99 THEN 'Ninguno'
    WHEN 98 THEN 'Extranjero en tránsito'
    END AS POBLACION_ESPECIAL,  
    a.APELLIDO1 AS PRIMER_APELLIDO,
	a.APELLIDO2 AS SEGUNDO_APELLIDO,
    a.NOMBRE1 AS PRIMER_NOMBRE,
	a.NOMBRE2 AS SEGUNDO_NOMBRE,
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
    CONCAT(e.CANTEDAD, ' ' +
    CASE e.FORMEDAD 
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    m.NOMBRE AS DIAGNOSTICO,
    n.NOMBRE AS CAUSA,
    CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
    CONVERT(varchar, CONVERT(DATE, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
    j.NOMBRE AS NOMBRE_SERVICIO
    FROM fac_m_tarjetero a 
    INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
    INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
    INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
    INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
    INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
    INNER JOIN fac_p_control g ON g.IPS = f.IPS 
    INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    INNER JOIN fac_m_procedimientos i ON i.IPS = e.IPS 
    AND i.DOCUMENTO = e.DOCUMENTO AND i.FACTURA = e.FACTURA
    INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    INNER JOIN fac_m_procedimientos_dx k ON k.IPS = i.IPS AND k.DOCUMENTO = i.DOCUMENTO 
    AND k.FACTURA = i.FACTURA AND K.CODIGO = I.CODIGO AND k.ATENCION = i.ATENCION
    INNER JOIN fac_p_diagnostico m ON m.DIAGNOSTICO = k.DIAGNOSTICO
    INNER JOIN fac_p_causa n ON n.CODIGO = k.CAUSA
    INNER JOIN fac_m_usuarios890 ñ ON ñ.HISTORIA = a.HISTORIA
    INNER JOIN gen_p_eps o ON o.CODIGO = ñ.EMPRESA
    INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
    AND f.ESTADO = 1
    AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
    
    ;WITH Duplicates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADO) AS RowNum
        FROM #TempResultsPivoted
    )
    DELETE FROM Duplicates WHERE RowNum > 1;

    DECLARE @ServiceNames NVARCHAR(MAX);
    SELECT @ServiceNames = COALESCE(@ServiceNames + ', ', '') + QUOTENAME(NOMBRE_SERVICIO)
    FROM #TempResultsPivoted
    GROUP BY NOMBRE_SERVICIO;

    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT *
    FROM (
        SELECT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, PRIMER_APELLIDO, SEGUNDO_APELLIDO, 
				PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, 
				GRADO_DISCAPACIDAD, DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
				BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
				MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
    MAX(DescripcionServicio) 
    FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';

    EXEC sp_executesql @SqlQuery;

    DROP TABLE #TempResultsPivoted`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results, sheetName) {
    const workbook = new ExcelJS.Workbook();
    
    // Agregar una hoja de Excel y establecer su nombre
    const worksheet = workbook.addWorksheet(sheetName);

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
        const sheetName = 'ADOLESCENCIA'; // Personaliza el nombre de la hoja aquí
        const excelTitle = 'CursoVida-Adolescencia'; // Personaliza el título del libro de Excel aquí

        // Ejecutar la consulta SQL y obtener los resultados
        const results = await executeSQLQuery2(CODIGO, edadInicial, edadFinal);

        // Llama a la función configureExcelWorkbook con los resultados y el nombre de la hoja como parámetros
        const workbook = configureExcelWorkbook(results, sheetName);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${excelTitle}.xlsx`
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
async function executeSQLQuery3(CODIGO, edadInicial, edadFinal) {
    const query = `
    DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
    DECLARE @FechaFinal DATE = GETDATE();

    CREATE TABLE #TempResultsPivoted (
    DUPLICADO NVARCHAR(255), 
    NOMBRE_IPS NVARCHAR(255),
	COD_EPS NVARCHAR(255),
	NOM_ASEGURADORA NVARCHAR(255),
    TIPO_DOCUMENTO NVARCHAR(255),
    POBLACION_ESPECIAL NVARCHAR(255), 
    PRIMER_APELLIDO NVARCHAR(255),
	SEGUNDO_APELLIDO NVARCHAR(255),
    PRIMER_NOMBRE NVARCHAR(255),
	SEGUNDO_NOMBRE NVARCHAR(255),
    FECHA_NACIMIENTO DATE,  
    GENERO NVARCHAR(50), 
    EMBARAZO NVARCHAR(10), 
    TIPO_DISCAPACIDAD NVARCHAR(50), 
    GRADO_DISCAPACIDAD NVARCHAR(50), 
    DIRECCION_RESIDENCIA NVARCHAR(255), 
    TELEFONO_RESIDENCIA NVARCHAR(20), 
    CODIGO_BARRIO NVARCHAR(255),  
    COMUNA NVARCHAR(255), 
    BARRIO NVARCHAR(255), 
    ETNICO NVARCHAR(50), 
    PAIS NVARCHAR(255),  
    CENTRO_PRODUCCION NVARCHAR(255), 
    CODIGO NVARCHAR(255),  
    ATENDIDO NVARCHAR(5), 
    ATENCION_FACTURA DATE, 
    ESTADO NVARCHAR(50), 
    DIA NVARCHAR(5),  
    MES NVARCHAR(5),  
    AÑO NVARCHAR(5),  
    EDAD NVARCHAR(50),
	DIAGNOSTICO NVARCHAR(255),
	CAUSA NVARCHAR(255),
    NUMDOCUM NVARCHAR(255),
    DescripcionServicio NVARCHAR(255),
    NOMBRE_SERVICIO NVARCHAR(255)
);

    INSERT INTO #TempResultsPivoted
    SELECT
    ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM,  g.NOMBRE, ñ.EMPRESA, o.NOMBRE, a.TIPDOCUM,  q.POBLACION_ESPECIAL, a.APELLIDO1, a.APELLIDO2,
                        a.NOMBRE1, a.NOMBRE2, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                        a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                        a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO, m.NOMBRE, n.NOMBRE
	ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
    g.NOMBRE AS NOMBRE_IPS,
	ñ.EMPRESA AS COD_EPS,
	o.NOMBRE AS NOM_ASEGURADORA,
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
    CASE q.POBLACION_ESPECIAL
    WHEN 1 THEN 'Personas de la tercera edad en protección de ancianatos'
    WHEN 2 THEN 'Indigenas mayor de edad'
    WHEN 3 THEN 'Habitante de la calle mayor de edad'
    WHEN 4 THEN 'Habitante de la calle menor de edad'
    WHEN 5 THEN 'Menor de edad desvinculado del conflicto armado'
    WHEN 6 THEN 'Población infantil vulnerable en instituciones diferentes al ICBF'
    WHEN 7 THEN 'Población infantil vulnerable a cargo del ICBF'
    WHEN 8 THEN 'Indigena menor de edad'
    WHEN 9 THEN 'Recien nacido con edad menor o igual a 30 días'
    WHEN 10 THEN 'Menor de edad desplazado'
    WHEN 11 THEN 'Mayor de edad desplazado'
    WHEN 12 THEN 'Recluso menor de edad'
    WHEN 13 THEN 'Recluso mayor de edad'
    WHEN 99 THEN 'Ninguno'
    WHEN 98 THEN 'Extranjero en tránsito'
    END AS POBLACION_ESPECIAL,  
    a.APELLIDO1 AS PRIMER_APELLIDO,
	a.APELLIDO2 AS SEGUNDO_APELLIDO,
    a.NOMBRE1 AS PRIMER_NOMBRE,
	a.NOMBRE2 AS SEGUNDO_NOMBRE,
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
    CONCAT(e.CANTEDAD, ' ' +
    CASE e.FORMEDAD 
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    m.NOMBRE AS DIAGNOSTICO,
    n.NOMBRE AS CAUSA,
    CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
    CONVERT(varchar, CONVERT(DATE, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
    j.NOMBRE AS NOMBRE_SERVICIO
    FROM fac_m_tarjetero a 
    INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
    INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
    INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
    INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
    INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
    INNER JOIN fac_p_control g ON g.IPS = f.IPS 
    INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    INNER JOIN fac_m_procedimientos i ON i.IPS = e.IPS 
    AND i.DOCUMENTO = e.DOCUMENTO AND i.FACTURA = e.FACTURA
    INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    INNER JOIN fac_m_procedimientos_dx k ON k.IPS = i.IPS AND k.DOCUMENTO = i.DOCUMENTO 
    AND k.FACTURA = i.FACTURA AND K.CODIGO = I.CODIGO AND k.ATENCION = i.ATENCION
    INNER JOIN fac_p_diagnostico m ON m.DIAGNOSTICO = k.DIAGNOSTICO
    INNER JOIN fac_p_causa n ON n.CODIGO = k.CAUSA
    INNER JOIN fac_m_usuarios890 ñ ON ñ.HISTORIA = a.HISTORIA
    INNER JOIN gen_p_eps o ON o.CODIGO = ñ.EMPRESA
    INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
    AND f.ESTADO = 1
    AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
    
    ;WITH Duplicates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADO) AS RowNum
        FROM #TempResultsPivoted
    )
    DELETE FROM Duplicates WHERE RowNum > 1;

    DECLARE @ServiceNames NVARCHAR(MAX);
    SELECT @ServiceNames = COALESCE(@ServiceNames + ', ', '') + QUOTENAME(NOMBRE_SERVICIO)
    FROM #TempResultsPivoted
    GROUP BY NOMBRE_SERVICIO;

    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT *
    FROM (
        SELECT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, PRIMER_APELLIDO, SEGUNDO_APELLIDO, 
				PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, 
				GRADO_DISCAPACIDAD, DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
				BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
				MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
    MAX(DescripcionServicio) 
    FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';

    EXEC sp_executesql @SqlQuery;

    DROP TABLE #TempResultsPivoted`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results, sheetName) {
    const workbook = new ExcelJS.Workbook();
    
    // Agregar una hoja de Excel y establecer su nombre
    const worksheet = workbook.addWorksheet(sheetName);

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
exports.cursoVidaExcel3 = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const sheetName = 'JUVENTUD'; // Personaliza el nombre de la hoja aquí
        const excelTitle = 'CursoVida-Juventud'; // Personaliza el título del libro de Excel aquí

        // Ejecutar la consulta SQL y obtener los resultados
        const results = await executeSQLQuery3(CODIGO, edadInicial, edadFinal);

        // Llama a la función configureExcelWorkbook con los resultados y el nombre de la hoja como parámetros
        const workbook = configureExcelWorkbook(results, sheetName);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${excelTitle}.xlsx`
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
async function executeSQLQuery4(CODIGO, edadInicial, edadFinal) {
    const query = `
    DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
    DECLARE @FechaFinal DATE = GETDATE();

    CREATE TABLE #TempResultsPivoted (
    DUPLICADO NVARCHAR(255), 
    NOMBRE_IPS NVARCHAR(255),
	COD_EPS NVARCHAR(255),
	NOM_ASEGURADORA NVARCHAR(255),
    TIPO_DOCUMENTO NVARCHAR(255),
    POBLACION_ESPECIAL NVARCHAR(255), 
    PRIMER_APELLIDO NVARCHAR(255),
	SEGUNDO_APELLIDO NVARCHAR(255),
    PRIMER_NOMBRE NVARCHAR(255),
	SEGUNDO_NOMBRE NVARCHAR(255),
    FECHA_NACIMIENTO DATE,  
    GENERO NVARCHAR(50), 
    EMBARAZO NVARCHAR(10), 
    TIPO_DISCAPACIDAD NVARCHAR(50), 
    GRADO_DISCAPACIDAD NVARCHAR(50), 
    DIRECCION_RESIDENCIA NVARCHAR(255), 
    TELEFONO_RESIDENCIA NVARCHAR(20), 
    CODIGO_BARRIO NVARCHAR(255),  
    COMUNA NVARCHAR(255), 
    BARRIO NVARCHAR(255), 
    ETNICO NVARCHAR(50), 
    PAIS NVARCHAR(255),  
    CENTRO_PRODUCCION NVARCHAR(255), 
    CODIGO NVARCHAR(255),  
    ATENDIDO NVARCHAR(5), 
    ATENCION_FACTURA DATE, 
    ESTADO NVARCHAR(50), 
    DIA NVARCHAR(5),  
    MES NVARCHAR(5),  
    AÑO NVARCHAR(5),  
    EDAD NVARCHAR(50),
	DIAGNOSTICO NVARCHAR(255),
	CAUSA NVARCHAR(255),
    NUMDOCUM NVARCHAR(255),
    DescripcionServicio NVARCHAR(255),
    NOMBRE_SERVICIO NVARCHAR(255)
);

    INSERT INTO #TempResultsPivoted
    SELECT
    ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM,  g.NOMBRE, ñ.EMPRESA, o.NOMBRE, a.TIPDOCUM,  q.POBLACION_ESPECIAL, a.APELLIDO1, a.APELLIDO2,
                        a.NOMBRE1, a.NOMBRE2, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                        a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                        a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO, m.NOMBRE, n.NOMBRE
	ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
    g.NOMBRE AS NOMBRE_IPS,
	ñ.EMPRESA AS COD_EPS,
	o.NOMBRE AS NOM_ASEGURADORA,
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
    CASE q.POBLACION_ESPECIAL
    WHEN 1 THEN 'Personas de la tercera edad en protección de ancianatos'
    WHEN 2 THEN 'Indigenas mayor de edad'
    WHEN 3 THEN 'Habitante de la calle mayor de edad'
    WHEN 4 THEN 'Habitante de la calle menor de edad'
    WHEN 5 THEN 'Menor de edad desvinculado del conflicto armado'
    WHEN 6 THEN 'Población infantil vulnerable en instituciones diferentes al ICBF'
    WHEN 7 THEN 'Población infantil vulnerable a cargo del ICBF'
    WHEN 8 THEN 'Indigena menor de edad'
    WHEN 9 THEN 'Recien nacido con edad menor o igual a 30 días'
    WHEN 10 THEN 'Menor de edad desplazado'
    WHEN 11 THEN 'Mayor de edad desplazado'
    WHEN 12 THEN 'Recluso menor de edad'
    WHEN 13 THEN 'Recluso mayor de edad'
    WHEN 99 THEN 'Ninguno'
    WHEN 98 THEN 'Extranjero en tránsito'
    END AS POBLACION_ESPECIAL,  
    a.APELLIDO1 AS PRIMER_APELLIDO,
	a.APELLIDO2 AS SEGUNDO_APELLIDO,
    a.NOMBRE1 AS PRIMER_NOMBRE,
	a.NOMBRE2 AS SEGUNDO_NOMBRE,
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
    CONCAT(e.CANTEDAD, ' ' +
    CASE e.FORMEDAD 
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    m.NOMBRE AS DIAGNOSTICO,
    n.NOMBRE AS CAUSA,
    CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
    CONVERT(varchar, CONVERT(DATE, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
    j.NOMBRE AS NOMBRE_SERVICIO
    FROM fac_m_tarjetero a 
    INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
    INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
    INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
    INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
    INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
    INNER JOIN fac_p_control g ON g.IPS = f.IPS 
    INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    INNER JOIN fac_m_procedimientos i ON i.IPS = e.IPS 
    AND i.DOCUMENTO = e.DOCUMENTO AND i.FACTURA = e.FACTURA
    INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    INNER JOIN fac_m_procedimientos_dx k ON k.IPS = i.IPS AND k.DOCUMENTO = i.DOCUMENTO 
    AND k.FACTURA = i.FACTURA AND K.CODIGO = I.CODIGO AND k.ATENCION = i.ATENCION
    INNER JOIN fac_p_diagnostico m ON m.DIAGNOSTICO = k.DIAGNOSTICO
    INNER JOIN fac_p_causa n ON n.CODIGO = k.CAUSA
    INNER JOIN fac_m_usuarios890 ñ ON ñ.HISTORIA = a.HISTORIA
    INNER JOIN gen_p_eps o ON o.CODIGO = ñ.EMPRESA
    INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
    AND f.ESTADO = 1
    AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
    
    ;WITH Duplicates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADO) AS RowNum
        FROM #TempResultsPivoted
    )
    DELETE FROM Duplicates WHERE RowNum > 1;

    DECLARE @ServiceNames NVARCHAR(MAX);
    SELECT @ServiceNames = COALESCE(@ServiceNames + ', ', '') + QUOTENAME(NOMBRE_SERVICIO)
    FROM #TempResultsPivoted
    GROUP BY NOMBRE_SERVICIO;

    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT *
    FROM (
        SELECT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, PRIMER_APELLIDO, SEGUNDO_APELLIDO, 
				PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, 
				GRADO_DISCAPACIDAD, DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
				BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
				MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
    MAX(DescripcionServicio) 
    FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';

    EXEC sp_executesql @SqlQuery;

    DROP TABLE #TempResultsPivoted`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

// Función para configurar el libro de Excel con los encabezados y datos
function configureExcelWorkbook(results, sheetName) {
    const workbook = new ExcelJS.Workbook();
    
    // Agregar una hoja de Excel y establecer su nombre
    const worksheet = workbook.addWorksheet(sheetName);

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
exports.cursoVidaExcel4 = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const sheetName = 'ADULTEZ'; // Personaliza el nombre de la hoja aquí
        const excelTitle = 'CursoVida-Adultez'; // Personaliza el título del libro de Excel aquí

        // Ejecutar la consulta SQL y obtener los resultados
        const results = await executeSQLQuery4(CODIGO, edadInicial, edadFinal);

        // Llama a la función configureExcelWorkbook con los resultados y el nombre de la hoja como parámetros
        const workbook = configureExcelWorkbook(results, sheetName);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${excelTitle}.xlsx`
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
async function executeSQLQuery5(CODIGO, edadInicial, edadFinal) {
    const query = `
    DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
    DECLARE @FechaFinal DATE = GETDATE();

    CREATE TABLE #TempResultsPivoted (
    DUPLICADO NVARCHAR(255), 
    NOMBRE_IPS NVARCHAR(255),
	COD_EPS NVARCHAR(255),
	NOM_ASEGURADORA NVARCHAR(255),
    TIPO_DOCUMENTO NVARCHAR(255),
    POBLACION_ESPECIAL NVARCHAR(255), 
    PRIMER_APELLIDO NVARCHAR(255),
	SEGUNDO_APELLIDO NVARCHAR(255),
    PRIMER_NOMBRE NVARCHAR(255),
	SEGUNDO_NOMBRE NVARCHAR(255),
    FECHA_NACIMIENTO DATE,  
    GENERO NVARCHAR(50), 
    EMBARAZO NVARCHAR(10), 
    TIPO_DISCAPACIDAD NVARCHAR(50), 
    GRADO_DISCAPACIDAD NVARCHAR(50), 
    DIRECCION_RESIDENCIA NVARCHAR(255), 
    TELEFONO_RESIDENCIA NVARCHAR(20), 
    CODIGO_BARRIO NVARCHAR(255),  
    COMUNA NVARCHAR(255), 
    BARRIO NVARCHAR(255), 
    ETNICO NVARCHAR(50), 
    PAIS NVARCHAR(255),  
    CENTRO_PRODUCCION NVARCHAR(255), 
    CODIGO NVARCHAR(255),  
    ATENDIDO NVARCHAR(5), 
    ATENCION_FACTURA DATE, 
    ESTADO NVARCHAR(50), 
    DIA NVARCHAR(5),  
    MES NVARCHAR(5),  
    AÑO NVARCHAR(5),  
    EDAD NVARCHAR(50),
	DIAGNOSTICO NVARCHAR(255),
	CAUSA NVARCHAR(255),
    NUMDOCUM NVARCHAR(255),
    DescripcionServicio NVARCHAR(255),
    NOMBRE_SERVICIO NVARCHAR(255)
);

    INSERT INTO #TempResultsPivoted
    SELECT
    ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM,  g.NOMBRE, ñ.EMPRESA, o.NOMBRE, a.TIPDOCUM,  q.POBLACION_ESPECIAL, a.APELLIDO1, a.APELLIDO2,
                        a.NOMBRE1, a.NOMBRE2, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                        a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                        a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO, m.NOMBRE, n.NOMBRE
	ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
    g.NOMBRE AS NOMBRE_IPS,
	ñ.EMPRESA AS COD_EPS,
	o.NOMBRE AS NOM_ASEGURADORA,
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
    CASE q.POBLACION_ESPECIAL
    WHEN 1 THEN 'Personas de la tercera edad en protección de ancianatos'
    WHEN 2 THEN 'Indigenas mayor de edad'
    WHEN 3 THEN 'Habitante de la calle mayor de edad'
    WHEN 4 THEN 'Habitante de la calle menor de edad'
    WHEN 5 THEN 'Menor de edad desvinculado del conflicto armado'
    WHEN 6 THEN 'Población infantil vulnerable en instituciones diferentes al ICBF'
    WHEN 7 THEN 'Población infantil vulnerable a cargo del ICBF'
    WHEN 8 THEN 'Indigena menor de edad'
    WHEN 9 THEN 'Recien nacido con edad menor o igual a 30 días'
    WHEN 10 THEN 'Menor de edad desplazado'
    WHEN 11 THEN 'Mayor de edad desplazado'
    WHEN 12 THEN 'Recluso menor de edad'
    WHEN 13 THEN 'Recluso mayor de edad'
    WHEN 99 THEN 'Ninguno'
    WHEN 98 THEN 'Extranjero en tránsito'
    END AS POBLACION_ESPECIAL,  
    a.APELLIDO1 AS PRIMER_APELLIDO,
	a.APELLIDO2 AS SEGUNDO_APELLIDO,
    a.NOMBRE1 AS PRIMER_NOMBRE,
	a.NOMBRE2 AS SEGUNDO_NOMBRE,
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
    CONCAT(e.CANTEDAD, ' ' +
    CASE e.FORMEDAD 
    WHEN 1 THEN 'AÑOS'
    WHEN 2 THEN 'MESES'
    WHEN 3 THEN 'DIAS'
    WHEN 4 THEN 'HORAS'
    END) AS EDAD,
    m.NOMBRE AS DIAGNOSTICO,
    n.NOMBRE AS CAUSA,
    CONVERT(NVARCHAR(255), a.NUMDOCUM) AS NUMDOCUM,
    CONVERT(varchar, CONVERT(DATE, f.ATENCION_FACTURA), 120) + '  //  '  + j.CODIGO AS DescripcionServicio,
    j.NOMBRE AS NOMBRE_SERVICIO
    FROM fac_m_tarjetero a 
    INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
    INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
    INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
    INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA 
    INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
    INNER JOIN fac_p_control g ON g.IPS = f.IPS 
    INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
    INNER JOIN fac_m_procedimientos i ON i.IPS = e.IPS 
    AND i.DOCUMENTO = e.DOCUMENTO AND i.FACTURA = e.FACTURA
    INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
    INNER JOIN fac_m_procedimientos_dx k ON k.IPS = i.IPS AND k.DOCUMENTO = i.DOCUMENTO 
    AND k.FACTURA = i.FACTURA AND K.CODIGO = I.CODIGO AND k.ATENCION = i.ATENCION
    INNER JOIN fac_p_diagnostico m ON m.DIAGNOSTICO = k.DIAGNOSTICO
    INNER JOIN fac_p_causa n ON n.CODIGO = k.CAUSA
    INNER JOIN fac_m_usuarios890 ñ ON ñ.HISTORIA = a.HISTORIA
    INNER JOIN gen_p_eps o ON o.CODIGO = ñ.EMPRESA
    INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL
    WHERE h.CODIGO = ${CODIGO}
    AND e.CANTEDAD BETWEEN ${edadInicial} AND ${edadFinal}
    AND f.ATENCION_FACTURA IS NOT NULL
    AND f.CODIGO_CUPS IS NOT NULL
    AND f.ATENDIDO = 1
    AND f.ESTADO = 1
    AND f.ATENCION_FACTURA BETWEEN @FechaInicial AND @FechaFinal;
    
    ;WITH Duplicates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NUMDOCUM, NOMBRE_SERVICIO ORDER BY DUPLICADO) AS RowNum
        FROM #TempResultsPivoted
    )
    DELETE FROM Duplicates WHERE RowNum > 1;

    DECLARE @ServiceNames NVARCHAR(MAX);
    SELECT @ServiceNames = COALESCE(@ServiceNames + ', ', '') + QUOTENAME(NOMBRE_SERVICIO)
    FROM #TempResultsPivoted
    GROUP BY NOMBRE_SERVICIO;

    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT *
    FROM (
        SELECT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, PRIMER_APELLIDO, SEGUNDO_APELLIDO, 
				PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, 
				GRADO_DISCAPACIDAD, DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
				BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
				MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
    MAX(DescripcionServicio) 
    FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';

    EXEC sp_executesql @SqlQuery;

    DROP TABLE #TempResultsPivoted`;
    const results = await sequelize.query(query, {
        type: QueryTypes.SELECT,
    });
    return results;
}

    // Función para configurar el libro de Excel con los encabezados y datos
    function configureExcelWorkbook(results, sheetName) {
    const workbook = new ExcelJS.Workbook();
    
    // Agregar una hoja de Excel y establecer su nombre
    const worksheet = workbook.addWorksheet(sheetName);

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
    exports.cursoVidaExcel5 = async (req, res) => {
    try {
        const CODIGO = req.params.CODIGO;
        const edadInicial = req.params.edadInicial;
        const edadFinal = req.params.edadFinal;
        const sheetName = 'VEJEZ'; // Personaliza el nombre de la hoja aquí
        const excelTitle = 'CursoVida-Vejez'; // Personaliza el título del libro de Excel aquí

        // Ejecutar la consulta SQL y obtener los resultados
        const results = await executeSQLQuery5(CODIGO, edadInicial, edadFinal);

        // Llama a la función configureExcelWorkbook con los resultados y el nombre de la hoja como parámetros
        const workbook = configureExcelWorkbook(results, sheetName);

        // Establecer las cabeceras de respuesta para descargar el archivo de Excel
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${excelTitle}.xlsx`
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
