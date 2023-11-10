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
        const query = `
        DECLARE @FechaInicial DATE = DATEADD(YEAR, -5, GETDATE());
        DECLARE @FechaFinal DATE = GETDATE();
    
            CREATE TABLE #TempResults (
                DUPLICADOS NVARCHAR(255), 
                IPS NVARCHAR(255), 
                NOMBRE_IPS NVARCHAR(255),
                TIPO_DOCUMENTO NVARCHAR(255),
                POBLACION_ESPECIAL NVARCHAR(255), 
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
                ROW_NUMBER() OVER (PARTITION BY a.NUMDOCUM, f.IPS, g.NOMBRE, a.TIPDOCUM, q.POBLACION_ESPECIAL, a.APELLIDO1,
                                                a.NOMBRE1, a.FECHANAC, a.SEXO, e.EMBARAZO, a.TIPDISCAP, a.GRDDISCAP, a.DIRECRES, a.TELEFRES, 
                                                a.CODBARES, d.NOMBRE, c.NOMBRE, a.ETNICO, b.NOMBRE, h.NOMBRE, f.CODIGO, f.ATENDIDO, f.ESTADO,
                                                a.FECHANAC, a.FECHANAC, a.FECHANAC, e.CANTEDAD, e.FORMEDAD, f.ATENCION_FACTURA, j.CODIGO
                ORDER BY a.NUMDOCUM DESC) AS DUPLICADO,
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
                INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA AND e.BARRIORES = c.CODIGO
                INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
                INNER JOIN fac_p_control g ON g.IPS = f.IPS AND g.IPS = e.IPS AND g.IPS = f.IPS
                INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD AND h.CODIGO = e.CENTROPRODUCCION
                AND h.CODIGO = f.CENTROPROD  
                INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
                INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL 
                AND q.POBLACION_ESPECIAL = e.POBLACION_ESPECIAL
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
            FROM #TempResults
            ORDER BY NUMDOCUM DESC
            OFFSET ${offset} ROWS
            FETCH NEXT ${limit} ROWS ONLY;

    
            SELECT COUNT(*) AS dato
            FROM #TempResults;
    
            DROP TABLE #TempResults;`;
        const resul = await sequelize.query(query, {type: QueryTypes.SELECT});
        
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            data: resul, 
            total: resul[resul.length - 1].dato,
            totalpage: Math.ceil(resul[resul.length - 1].dato / limit)
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
            INNER JOIN gen_p_paises b ON b.PAIS = a.PAIS  
            INNER JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES  
            INNER JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA  
            INNER JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA AND e.BARRIORES = c.CODIGO
            INNER JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA 
            INNER JOIN fac_p_control g ON g.IPS = f.IPS AND g.IPS = e.IPS AND g.IPS = f.IPS
            INNER JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD AND h.CODIGO = e.CENTROPRODUCCION
            AND h.CODIGO = f.CENTROPROD  
            INNER JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
            INNER JOIN fac_p_poblacion_especial q ON q.POBLACION_ESPECIAL = a.POBLACION_ESPECIAL 
            AND q.POBLACION_ESPECIAL = e.POBLACION_ESPECIAL 
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
    SELECT @ServiceNames = N'[ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD]'
    + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO]' + N', [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD]'
    + N', [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL]' + N', [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL]'
    + N', [CONSULTA DE PRIMERA VEZ POR ENFERMERIA]' + N', [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA]'
    + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN PEDIATRÍA]'
    + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR MEDICINA GENERAL]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL]'
    + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA]'
    + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN PEDIATRÍA]'
    + N', [HEMOGLOBINA]' + N', [MEDICION DE AGUDEZA VISUAL]'
    + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL]' + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA]'
    + N', [EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA]' + N', [EDUCACION GRUPAL EN SALUD, POR ENFERMERIA]'
    + N', [EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA]' + N', [EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA]'
    + N', [EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL]' + N', [EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA]'
    + N', [EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL]' + N', [EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA]'
    + N', [EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO]' + N', [EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL]'
    + N', [EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA]'
    + N', [EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA]'
    + N', [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA]'
    + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL]'
    + N', [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN INFANTIL Y ADOLESCENTE]'
    + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES]'
    + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR]' + N', [VACUNACION CONTRA TUBERCULOSIS BCG]'
    + N', [VACUNACIÓN CONTRA NEUMOCOCO]' + N', [VACUNACIÓN COMBINADA CONTRA DIFTERIA, TÉTANOS Y TOS FERINA DPT]'
    + N', [VACUNACIÓN COMBINADA CONTRA HAEMOPHILUS INFLUENZA TIPO B, DIFTERIA, TÉTANOS, TOS FERINA Y HEPATITIS B PENTAVALENTE]' + N', [VACUNACION CONTRA POLIOMIELITIS (VOP O IVP)]'
    + N', [VACUNACIÓN CONTRA POLIOMIELITIS (VOP O IVP) INTRAMUSCULAR]' + N', [VACUNACION CONTRA HEPATITIS A]'
    + N', [VACUNACION CONTRA Hepatitis B]' + N', [VACUNACION CONTRA FIEBRE AMARILLA]'
    + N', [VACUNACION CONTRA VARICELA]' + N', [VACUNA CONTRA INFLUENZA PEDIATRICA  (H1N1)]'
    + N', [VACUNACION CONTRA ROTAVIRUS]' + N', [VACUNACIÓN COMBINADA CONTRA SARAMPIÓN, PAROTIDITIS Y RUBÉOLA SRP TRIPLE VIRAL]'
    + N', [APLICACION DE SELLANTES DE FOTOCURADO]' + N', [TOPICACIÓN DE FLUOR EN BARNIZ]'
    + N', [CONTROL DE PLACA DENTAL -]' + N', [CONSEJERIA EN VIH]'
    + N', [DEMANDA INDUCIDA CANALIZADOS ALTERACIONES DE LA AGUDEZA VISUAL]' + N', [DEMANDA INDUCIDA CANALIZADOS ATENCION DEL PARTO]'
    + N', [DEMANDA INDUCIDA CANALIZADOS CRECIMIENTO Y DESARROLLO]' + N', [DEMANDA INDUCIDA CANALIZADOS VACUNACION PAI]'
    + N', [DEMANDA INDUCIDA CANALIZADOS VALORACION DEL JOVEN]';
                            
    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT DISTINCT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
        PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
        FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
        DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
        BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
        MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM,
        ' + @ServiceNames + '
    FROM (
        SELECT DISTINCT NUMDOCUM, NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
            PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
            FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
            DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
            BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
            MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
        MAX(DescripcionServicio) FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
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
    SET @ServiceNames = N'[ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA]'+ N', [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD]'
                    + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO]'+ N', [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD]'
                    + N', [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL]'+ N', [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL]'
                    + N', [CONSULTA DE PRIMERA VEZ POR ENFERMERIA]'+ N', [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA]'
                    + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR]'+ N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN PEDIATRÍA]'
                    + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR MEDICINA GENERAL]'+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL]'
                    + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA]'+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA]'
                    + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR]'+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN PEDIATRÍA]'
                    + N', [HEMATOCRITO]'+ N', [HEMOGLOBINA]'+ N', [MEDICION DE AGUDEZA VISUAL]'+ N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL]'
                    + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA]'+ N', [EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA]'
                    + N', [EDUCACION GRUPAL EN SALUD, POR ENFERMERIA]'+ N', [EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA]'
                    + N', [EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA]'+ N', [EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL]'
                    + N', [EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA]'+ N', [EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL]'
                    + N', [EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA]'+ N', [EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO]'
                    + N', [EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL]'+ N', [EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL]'
                    + N', [EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA]'+ N', [EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA]'
                    + N', [EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA]'+ N', [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL]'
                    + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA]'+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL]'
                    + N', [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL]'+ N', [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO]'
                    + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL]'
                    + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR]'
                    + N', [VACUNACION COMBINADA CONTRA TETANOS Y DIFTERIA TD]'+ N', [VACUNACION CONTRA INFLUENZA]'
                    + N', [VACUNACION CONTRA VIRUS PAPILOMA HUMANO VPH]'+ N', [VACUNACIÓN COMBINADA CONTRA SARAMPIÓN, PAROTIDITIS Y RUBÉOLA SRP TRIPLE VIRAL]'
                    + N', [APLICACION DE SELLANTES DE FOTOCURADO]'+ N', [TOPICACIÓN DE FLUOR EN BARNIZ]'
                    + N', [CONTROL DE PLACA DENTAL -]'+ N', [CONSEJERIA EN VIH]'+ N', [DEMANDA INDUCIDA CANALIZADOS ALTERACIONES DE LA AGUDEZA VISUAL]'
                    + N', [DEMANDA INDUCIDA CANALIZADOS ATENCION DEL PARTO]'+ N', [DEMANDA INDUCIDA CANALIZADOS CRECIMIENTO Y DESARROLLO]'
                    + N', [DEMANDA INDUCIDA CANALIZADOS VACUNACION PAI]'+ N', [DEMANDA INDUCIDA CANALIZADOS VALORACION DEL JOVEN]'
                    + N', [TREPONEMA PALLIDUM - ANTICUERPOS PARA SIFILIS - PRUEBA RAPIDA POR INMUNOCROMATOGRAFIA]';

                    PRINT @ServiceNames;
    
    DECLARE @SqlQuery NVARCHAR(MAX);
    SET @SqlQuery = '
    SELECT DISTINCT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
        PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
        FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
        DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
        BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
        MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM,
        ' + @ServiceNames + '
    FROM (
        SELECT DISTINCT NUMDOCUM, NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
            PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
            FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
            DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
            BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
            MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, DescripcionServicio, NOMBRE_SERVICIO
        FROM #TempResultsPivoted
    ) AS Source
    PIVOT (
        MAX(DescripcionServicio) FOR NOMBRE_SERVICIO IN (' + @ServiceNames + ')
    ) AS PivotTable';
    
    EXEC sp_executesql @SqlQuery;
    
    DROP TABLE #TempResultsPivoted;`;
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

    DECLARE @ServiceNamesPart1 NVARCHAR(MAX) = N' [INSERCION DE DISPOSITIVO INTRAUTERINO ANTICONCEPTIVO DIU SOD]' + N', [COLPOSCOPIA CON BIOPSIA]' 
                        + N', [INSERCION DE ANTICONCEPTIVOS SUBDERMICOS]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA]'
                        + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO]'
                        + N', [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD]' + N', [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL]'
                        + N', [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL]' + N', [CONSULTA DE PRIMERA VEZ POR ENFERMERIA]' + N', [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA]'
                        + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN GINECOLOGÍA Y OBSTETRICIA]' + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR]'  
                        + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN PEDIATRÍA]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR MEDICINA GENERAL]'
                        + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA]'
                        + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN GINECOLOGÍA Y OBSTETRICIA]' 
                        + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN PEDIATRÍA]'
                        + N', [TOMA NO QUIRURGICA DE MUESTRA O TEJIDO VAGINAL PARA ESTUDIO CITOLOGICO]' + N', [ESTUDIO DE COLORACION BASICA EN CITOLOGIA VAGINAL TUMORAL O FUNCIONAL]'
                        + N', [ESTUDIO DE COLORACION BASICA EN BIOPSIA]' + N', [HEMATOCRITO]' + N', [HEMOGLOBINA]' 
                        + N', [GONADOTROPINA CORIONICA SUBUNIDAD BETA CUALITATIVA PRUEBA DE EMBARAZO EN ORINA O SUERO]' + N', [TREPONEMA PALLIDUM ANTICUERPOS (PRUEBA TREPONEMICA) MANUAL O SEMIAUTOMATIZADA O AUTOMATIZADA]'
                        + N', [PRUEBA TREPONEMICA MANUAL RAPIDA DUO]' + N', [VIRUS DE INMUNODEFICIENCIA HUMANA 1 Y 2 ANTICUERPOS]' + N', [TAMIZAJE VIH]' + N', [MEDICION DE AGUDEZA VISUAL]'
                        + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL]' + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA]',
        @ServiceNamesPart2 NVARCHAR(MAX) = N', [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL]' + N',  [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA]'
                        + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL]'
                        + N', [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL]'
                        + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR]'
                        + N', [VACUNACION COMBINADA CONTRA TETANOS Y DIFTERIA TD]' + N', [VACUNACION COMBINADA CONTRA DIFTERIA,TETANOS Y TOS FERINA DPT  GESTANTE]'
                        + N', [VACUNACION CONTRA VIRUS PAPILOMA HUMANO VPH]' + N', [APLICACION DE SELLANTES DE FOTOCURADO]' + N', [TOPICACIÓN DE FLUOR EN BARNIZ]'
                        + N', [DETARTRAJE SUPRAGINGIVAL]' + N', [CONTROL DE PLACA DENTAL -]' + N', [CONSEJERIA EN VIH]' + N', [DEMANDA INDUCIDA CANALIZADOS ALTERACIONES DE LA AGUDEZA VISUAL]'
                        + N', [DEMANDA INDUCIDA CANALIZADOS ATENCION DEL PARTO]' + N', [DEMANDA INDUCIDA CANALIZADOS CRECIMIENTO Y DESARROLLO]' 
                        + N', [DEMANDA INDUCIDA CANALIZADOS VACUNACION PAI]' + N', [DEMANDA INDUCIDA CANALIZADOS VALORACION DEL JOVEN]' + N', [VIH - PRUEBA RAPIDA]' 
                        + N', [TREPONEMA PALLIDUM - ANTICUERPOS PARA SIFILIS - PRUEBA RAPIDA POR INMUNOCROMATOGRAFIA]';
        
        DECLARE @SqlQuery NVARCHAR(MAX);
        SET @SqlQuery = N'
        SELECT DISTINCT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
            PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
            FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
            DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
            BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
            MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM,
            ' + @ServiceNamesPart1 + @ServiceNamesPart2 + '
        FROM (
            SELECT DISTINCT NUMDOCUM, NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
                PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
                FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
                DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
                BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
                MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, DescripcionServicio, NOMBRE_SERVICIO
            FROM #TempResultsPivoted
        ) AS Source
        PIVOT (
            MAX(DescripcionServicio) FOR NOMBRE_SERVICIO IN (' + @ServiceNamesPart1 + @ServiceNamesPart2 + ')
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

    DECLARE @ServiceNamesPart1 NVARCHAR(MAX) = N'[VASECTOMÍA SOD]' + N', [ABLACIÓN U OCLUSIÓN DE TROMPA DE FALOPIO BILATERAL POR LAPAROTOMÍA]' 
	+ N', [ABLACIÓN U OCLUSIÓN DE TROMPA DE FALOPIO BILATERAL POR LAPAROSCOPIA]' + N', [INSERCION DE DISPOSITIVO INTRAUTERINO ANTICONCEPTIVO DIU SOD]' 
	+ N', [COLPOSCOPIA CON BIOPSIA]' + N', [INSERCION DE ANTICONCEPTIVOS SUBDERMICOS]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA]' 
	+ N', [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO]' 
	+ N', [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD]' + N', [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL]' 
	+ N', [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL]' + N', [CONSULTA DE PRIMERA VEZ POR ENFERMERIA]' + N', [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA]'
	+ N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN GINECOLOGÍA Y OBSTETRICIA]' + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR]' 
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR MEDICINA GENERAL]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL]'
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA]'
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN GINECOLOGÍA Y OBSTETRICIA]' 
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [TOMA NO QUIRURGICA DE MUESTRA O TEJIDO VAGINAL PARA ESTUDIO CITOLOGICO]'
	+ N', [ESTUDIO DE COLORACION BASICA EN CITOLOGIA VAGINAL TUMORAL O FUNCIONAL]' + N', [ESTUDIO DE COLORACION BASICA EN BIOPSIA]' + N', [COLESTEROL DE ALTA DENSIDAD]' 
	+ N', [COLESTEROL DE BAJA DENSIDAD SEMIAUTOMATIZADO]' + N', [COLESTEROL TOTAL]' + N', [GLUCOSA EN SUERO U OTRO FLUIDO DIFERENTE A ORINA]' + N', [TRIGLICERIDOS]' 
	+ N', [CREATININA EN SUERO U OTROS FLUIDOS]' + N', [GONADOTROPINA CORIONICA SUBUNIDAD BETA CUALITATIVA PRUEBA DE EMBARAZO EN ORINA O SUERO]' 
	+ N', [TREPONEMA PALLIDUM ANTICUERPOS (PRUEBA TREPONEMICA) MANUAL O SEMIAUTOMATIZADA O AUTOMATIZADA]' + N', [PRUEBA TREPONEMICA MANUAL RAPIDA DUO]', 
	@ServiceNamesPart2 NVARCHAR(MAX) = N', [VIRUS DE INMUNODEFICIENCIA HUMANA 1 Y 2 ANTICUERPOS]' + N', [TAMIZAJE VIH]' + N', [HEPATITIS C ANTICUERPOS MANUAL]' 
	+ N', [HEPATITIS B ANTIGENO DE SUPERFICIE AG HBS]' + N', [UROANALISIS]' + N', [MEDICION DE AGUDEZA VISUAL]' + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA]' + N', [EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR ENFERMERIA]' + N', [EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA]' + N', [EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL]' + N', [EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA]'
	+ N', [EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL]' + N', [EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA]'
	+ N', [EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO]' + N', [EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL]'
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA]'
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA]' + N', [CONSEJERIA Y ASESORIA "PRE" PARA VIH]' + N', [CONSEJERIA Y ASESORIA "POST" PARA VIH]'
	+ N', [TAMIZAJE DE RIESGO CARDIOVASCULAR]' + N', [TAMIZAJE DE VPH]' + N', [FORMULACIÓN Y ENTREGA DE MÉTODOS ANTICONCEPTIVOS]'
	+ N', [CONSEJERIA EN SALUD SEXUAL Y REPRODUCTIVA]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL]'
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR]'
	+ N', [VACUNACION COMBINADA CONTRA TETANOS Y DIFTERIA TD]' + N', [VACUNACION COMBINADA CONTRA DIFTERIA,TETANOS Y TOS FERINA DPT  GESTANTE]'
	+ N', [VACUNACION CONTRA INFLUENZA]' + N', [DETARTRAJE SUPRAGINGIVAL]' + N', [CONTROL DE PLACA DENTAL -]' + N', [VIH - PRUEBA RAPIDA]'
	+ N', [DENGUE - ANTICUERPOS IG G - IG M  - PRUEBA RAPIDA]' + N', [TREPONEMA PALLIDUM - ANTICUERPOS PARA SIFILIS - PRUEBA RAPIDA POR INMUNOCROMATOGRAFIA]'
	+ N', [DETECCION VIRUS DEL PAPILOMA HUMANO REACCION EN CADENA DE LA POLIMERASA - PRUEBA RAPIDA]';


    DECLARE @SqlQuery NVARCHAR(MAX);
        SET @SqlQuery = N'
        SELECT DISTINCT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
            PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
            FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
            DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
            BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
            MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM,
            ' + @ServiceNamesPart1 + @ServiceNamesPart2 + '
        FROM (
            SELECT DISTINCT NUMDOCUM, NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
                PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
                FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
                DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
                BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
                MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, DescripcionServicio, NOMBRE_SERVICIO
            FROM #TempResultsPivoted
        ) AS Source
        PIVOT (
            MAX(DescripcionServicio) FOR NOMBRE_SERVICIO IN (' + @ServiceNamesPart1 + @ServiceNamesPart2 + ')
        ) AS PivotTable';

        EXEC sp_executesql @SqlQuery;

        DROP TABLE #TempResultsPivoted;`;
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

    DECLARE @ServiceNamesPart1 NVARCHAR(MAX) = N'[COLONOSCOPIA TOTAL]' + N', [BIOPSIA CERRADA (ENDOSCÓPICA) DEL INTESTINO GRUESO SOD]' 
	+ N', [BIOPSIA CERRADA DE PRÓSTATA POR ABORDAJE TRANSRECTAL]' + N', [BIOPSIA CERRADA DE PRÓSTATA POR ABORDAJE PERINEAL]' 
	+ N', [VASECTOMÍA SOD]' + N', [ABLACIÓN U OCLUSIÓN DE TROMPA DE FALOPIO BILATERAL POR LAPAROTOMÍA]' 
	+ N', [ABLACIÓN U OCLUSIÓN DE TROMPA DE FALOPIO BILATERAL POR LAPAROSCOPIA]' + N', [CRIOCAUTERIZACION DE CUELLO UTERINO CERVIX]' 
	+ N', [INSERCION DE DISPOSITIVO INTRAUTERINO ANTICONCEPTIVO DIU SOD]' + N', [COLPOSCOPIA CON BIOPSIA]' 
	+ N', [BIOPSIA POR PUNCION CON AGUJA FINA DE MAMA]' + N', [BIOPSIA DE MAMA CON AGUJA (TRUCUT)]' 
	+ N', [INSERCION DE ANTICONCEPTIVOS SUBDERMICOS]' + N', [MAMOGRAFIA BILATERAL]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA]' 
	+ N', [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO]'
	+ N', [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD]' + N', [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL]' 
	+ N', [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL]' + N', [CONSULTA DE PRIMERA VEZ POR ENFERMERIA]' 
	+ N', [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA]' + N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN GINECOLOGÍA Y OBSTETRICIA]'
	+ N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL]' 
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA]' 
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN GINECOLOGÍA Y OBSTETRICIA]' 
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [TOMA NO QUIRURGICA DE MUESTRA O TEJIDO VAGINAL PARA ESTUDIO CITOLOGICO]'
	+ N', [TÉCNICAS DE INSPECCIÓN VISUAL CON ÁCIDO ACETICO Y LUGOL]' + N', [ESTUDIO DE COLORACION BASICA EN CITOLOGIA VAGINAL TUMORAL O FUNCIONAL]' 
	+ N', [ESTUDIO DE COLORACION BASICA EN BIOPSIA]' + N', [COLESTEROL DE ALTA DENSIDAD]' + N', [COLESTEROL DE BAJA DENSIDAD SEMIAUTOMATIZADO]' 
	+ N', [COLESTEROL TOTAL]' + N', [GLUCOSA EN SUERO U OTRO FLUIDO DIFERENTE A ORINA]' + N', [TRIGLICERIDOS]' + N', [CREATININA EN SUERO U OTROS FLUIDOS]'
	+ N', [GONADOTROPINA CORIONICA SUBUNIDAD BETA CUALITATIVA PRUEBA DE EMBARAZO EN ORINA O SUERO]' 
	+ N', [TREPONEMA PALLIDUM ANTICUERPOS (PRUEBA TREPONEMICA) MANUAL O SEMIAUTOMATIZADA O AUTOMATIZADA]' + N', [PRUEBA TREPONEMICA MANUAL RAPIDA DUO]'
	+ N', [VIRUS DE INMUNODEFICIENCIA HUMANA 1 Y 2 ANTICUERPOS]' + N', [TAMIZAJE VIH]' + N', [HEPATITIS C ANTICUERPOS MANUAL]' 
	+ N', [HEPATITIS B ANTIGENO DE SUPERFICIE AG HBS]' + N', [ANTIGENO ESPECIFICO DE PROSTATA SEMIAUTOMATIZADO O AUTOMATIZADO]', 
	@ServiceNamesPart2 NVARCHAR(MAX) = N', [SANGRE OCULTA EN MATERIA FECAL DETERMINACION DE HEMOGLOBINA HUMANA ESPECIFICA]' + N', [UROANALISIS]' 
	+ N', [DETECCION VIRUS DEL PAPILOMA HUMANO PRUEBAS DE ADN]' + N', [MEDICION DE AGUDEZA VISUAL]' + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA]' + N', [EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA]'
	+ N', [EDUCACION GRUPAL EN SALUD, POR ENFERMERIA]' + N', [EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA]' + N', [EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL]' 
	+ N', [EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA]' + N', [EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA]' + N', [EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA]' 
	+ N', [CONSEJERIA Y ASESORIA "PRE" PARA VIH]' + N', [CONSEJERIA Y ASESORIA "POST" PARA VIH]' + N', [TAMIZAJE DE RIESGO CARDIOVASCULAR]' 
	+ N', [TAMIZAJE DE VPH]' + N', [FORMULACIÓN Y ENTREGA DE MÉTODOS ANTICONCEPTIVOS]' + N', [CONSEJERIA EN SALUD SEXUAL Y REPRODUCTIVA]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR]' 
	+ N', [VACUNACION COMBINADA CONTRA TETANOS Y DIFTERIA TD]' + N', [DETARTRAJE SUPRAGINGIVAL]' + N', [CONTROL DE PLACA DENTAL -]' 
	+ N', [EXAMEN CLINICO DE MAM - DESH]' + N', [DESHABILITADO - TAMIZAJE CA PROSTATA POR TACTO RECTAL - DESH]' 
	+ N', [INFORMACION EDUCACION Y COMUNICACION EN POBLACION MUJERES GESTANTES Y LACTANTES]' + N', [TAMIZAJE CA PROSTATA POR TACTO RECTA]' 
	+ N', [VIH - PRUEBA RAPIDA]' + N', [DENGUE - ANTICUERPOS IG G - IG M  - PRUEBA RAPIDA]' 
	+ N', [TREPONEMA PALLIDUM - ANTICUERPOS PARA SIFILIS - PRUEBA RAPIDA POR INMUNOCROMATOGRAFIA]' 
	+ N', [DETECCION VIRUS DEL PAPILOMA HUMANO REACCION EN CADENA DE LA POLIMERASA - PRUEBA RAPIDA]';

    DECLARE @SqlQuery NVARCHAR(MAX);
        SET @SqlQuery = N'
        SELECT DISTINCT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
            PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
            FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
            DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
            BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
            MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM,
            ' + @ServiceNamesPart1 + @ServiceNamesPart2 + '
        FROM (
            SELECT DISTINCT NUMDOCUM, NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
                PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
                FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
                DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
                BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
                MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, DescripcionServicio, NOMBRE_SERVICIO
            FROM #TempResultsPivoted
        ) AS Source
        PIVOT (
            MAX(DescripcionServicio) FOR NOMBRE_SERVICIO IN (' + @ServiceNamesPart1 + @ServiceNamesPart2 + ')
        ) AS PivotTable';

        EXEC sp_executesql @SqlQuery;

        DROP TABLE #TempResultsPivoted;`;
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

    DECLARE @ServiceNamesPart1 NVARCHAR(MAX) = N'[COLONOSCOPIA TOTAL]' + N', [BIOPSIA CERRADA (ENDOSCÓPICA) DEL INTESTINO GRUESO SOD]'
	+ N', [BIOPSIA CERRADA DE PRÓSTATA POR ABORDAJE TRANSRECTAL]' + N', [BIOPSIA CERRADA DE PRÓSTATA POR ABORDAJE PERINEAL]'
	+ N', [VASECTOMÍA SOD]' + N', [COLPOSCOPIA CON BIOPSIA]' + N', [BIOPSIA POR PUNCION CON AGUJA FINA DE MAMA]' + N', [BIOPSIA DE MAMA CON AGUJA (TRUCUT)]'
	+ N', [INSERCION DE ANTICONCEPTIVOS SUBDERMICOS]' + N', [MAMOGRAFIA BILATERAL]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR ENFERMERÍA]' 
	+ N', [ATENCIÓN (VISITA) DOMICILIARIA, POR PROMOTOR DE LA SALUD]' + N', [ATENCIÓN (VISITA) DOMICILIARIA, POR EQUIPO INTERDISCIPLINARIO]' 
	+ N', [ATENCION (VISITA) DOMICILIARIA POR OTRO PROFESIONAL DE LA SALUD]' + N', [CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL]' 
	+ N', [CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL]' + N', [CONSULTA DE PRIMERA VEZ POR ENFERMERIA]' + N', [CONSULTA DE PRIMERA VEZ POR NUTRICION Y DIETETICA]'
	+ N', [CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ODONTOLOGIA GENERAL]' 
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ENFERMERIA]' + N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR NUTRICION Y DIETETICA]'
	+ N', [CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN MEDICINA FAMILIAR]' + N', [TOMA NO QUIRURGICA DE MUESTRA O TEJIDO VAGINAL PARA ESTUDIO CITOLOGICO]'
	+ N', [ESTUDIO DE COLORACION BASICA EN CITOLOGIA VAGINAL TUMORAL O FUNCIONAL]' + N', [ESTUDIO DE COLORACION BASICA EN BIOPSIA]' + N', [COLESTEROL DE ALTA DENSIDAD]'
	+ N', [COLESTEROL DE BAJA DENSIDAD SEMIAUTOMATIZADO]' + N', [COLESTEROL TOTAL]' + N', [GLUCOSA EN SUERO U OTRO FLUIDO DIFERENTE A ORINA]' + N', [TRIGLICERIDOS]' 
	+ N', [CREATININA EN SUERO U OTROS FLUIDOS]' + N', [TREPONEMA PALLIDUM ANTICUERPOS (PRUEBA TREPONEMICA) MANUAL O SEMIAUTOMATIZADA O AUTOMATIZADA]' 
	+ N', [PRUEBA TREPONEMICA MANUAL RAPIDA DUO]' + N', [VIRUS DE INMUNODEFICIENCIA HUMANA 1 Y 2 ANTICUERPOS]' + N', [TAMIZAJE VIH]' + N', [VIH SALUD OCUPACIONAL]'
	+ N', [HEPATITIS C ANTICUERPOS MANUAL]' + N', [HEPATITIS B ANTIGENO DE SUPERFICIE AG HBS]' + N', [ANTIGENO ESPECIFICO DE PROSTATA SEMIAUTOMATIZADO O AUTOMATIZADO]' 
	+ N', [SANGRE OCULTA EN MATERIA FECAL (DETERMINACION DE HEMOGLOBINA HUMANA ESPECIFICA)]' + N', [UROANALISIS]'
	+ N', [DETECCION VIRUS DEL PAPILOMA HUMANO PRUEBAS DE ADN]' + N', [MEDICION DE AGUDEZA VISUAL]' + N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA GENERAL]',
	@ServiceNamesPart2 NVARCHAR(MAX) = N', [EDUCACION GRUPAL EN SALUD, POR MEDICINA ESPECIALIZADA]' + N', [EDUCACION GRUPAL EN SALUD, POR ODONTOLOGIA]' + N', [EDUCACION GRUPAL EN SALUD, POR ENFERMERIA]'
	+ N', [EDUCACIÓN GRUPAL EN SALUD, POR NUTRICION Y DIETETICA]' + N', [EDUCACION GRUPAL EN SALUD, POR PSICOLOGIA]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR TRABAJO SOCIAL]' + N', [EDUCACIÓN GRUPAL EN SALUD, POR FISIOTERAPIA]' 
	+ N', [EDUCACIÓN GRUPAL EN SALUD, POR TERAPIA OCUPACIONAL]' + N', [EDUCACION GRUPAL EN SALUD, POR FONIATRIA Y FONOAUDIOLOGIA]' 
	+ N', [EDUCACION GRUPAL EN SALUD, POR AGENTE EDUCATIVO]' + N', [EDUCACION GRUPAL EN SALUD, POR HIGIENE ORAL]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR MEDICINA GENERAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR ODONTOLOGIA]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR ENFERMERIA]' + N', [CONSEJERIA Y ASESORIA "PRE" PARA VIH]' + N', [CONSEJERIA Y ASESORIA "POST" PARA VIH]' 
	+ N', [TAMIZAJE DE RIESGO CARDIOVASCULAR]' + N', [TAMIZAJE DE VPH]' + N', [FORMULACIÓN Y ENTREGA DE MÉTODOS ANTICONCEPTIVOS]' 
	+ N', [CONSEJERIA EN SALUD SEXUAL Y REPRODUCTIVA]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR PSICOLOGIA]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR TRABAJO SOCIAL]'
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR FISIOTERAPIA]' + N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR TERAPIA OCUPACIONAL]' 
	+ N', [EDUCACION INDIVIDUAL EN SALUD, POR HIGIENE ORAL]' + N', [EDUCACION INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO]' 
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE HOMBRES Y MUJERES EN EDAD FÉRTIL]'
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE MUJERES GESTANTES Y LACTANTES]'
	+ N', [EDUCACIÓN INDIVIDUAL EN SALUD, POR EQUIPO INTERDISCIPLINARIO Y COMUNICACIÓN EN POBLACIÓN DE ADULTO MAYOR]'
	+ N', [VACUNACION COMBINADA CONTRA TETANOS Y DIFTERIA TD]' + N', [DETARTRAJE SUPRAGINGIVAL]' + N', [CONTROL DE PLACA DENTAL -]' 
	+ N', [EXAMEN CLINICO DE MAM - DESH]' + N', [DESHABILITADO - TAMIZAJE CA PROSTATA POR TACTO RECTAL - DESH]' 
	+ N', [INFORMACION EDUCACION Y COMUNICACION EN POBLACION MUJERES GESTANTES Y LACTANTES]' + N', [TAMIZAJE CA PROSTATA POR TACTO RECTA]' 
	+ N', [EXAMEN CLINICO DE MAMA]' + N', [VIH - PRUEBA RAPIDA]' + N', [TREPONEMA PALLIDUM - ANTICUERPOS PARA SIFILIS - PRUEBA RAPIDA POR INMUNOCROMATOGRAFIA]';

DECLARE @SqlQuery NVARCHAR(MAX);
        SET @SqlQuery = N'
        SELECT DISTINCT NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
            PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
            FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
            DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
            BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
            MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, NUMDOCUM,
            ' + @ServiceNamesPart1 + @ServiceNamesPart2 + '
        FROM (
            SELECT DISTINCT NUMDOCUM, NOMBRE_IPS, COD_EPS, NOM_ASEGURADORA, TIPO_DOCUMENTO, POBLACION_ESPECIAL, 
                PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
                FECHA_NACIMIENTO, GENERO, EMBARAZO, TIPO_DISCAPACIDAD, GRADO_DISCAPACIDAD, 
                DIRECCION_RESIDENCIA, TELEFONO_RESIDENCIA, CODIGO_BARRIO, COMUNA, 
                BARRIO, ETNICO, PAIS, CENTRO_PRODUCCION, CODIGO, ATENDIDO, ESTADO, DIA, 
                MES, AÑO, EDAD, DIAGNOSTICO, CAUSA, DescripcionServicio, NOMBRE_SERVICIO
            FROM #TempResultsPivoted
        ) AS Source
        PIVOT (
            MAX(DescripcionServicio) FOR NOMBRE_SERVICIO IN (' + @ServiceNamesPart1 + @ServiceNamesPart2 + ')
        ) AS PivotTable';

        EXEC sp_executesql @SqlQuery;

        DROP TABLE #TempResultsPivoted;`;
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
