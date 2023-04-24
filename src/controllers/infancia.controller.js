const { QueryTypes } = require('sequelize');
const sequelize = require('../BD-RFAS8/conexiondb');


exports.infancia = async (req, res) => {
    try {
        const page = req.query.page || 1;
        const limit = req.query.limit || 10;
        const offset = (page - 1) * limit;
        const query = `SELECT 
        f.IPS, g.NOMBRE AS NOMBRE_IPS, 
        a.TIPDOCUM AS TIPO_DOCUMENTO, 
        a.NUMDOCUM AS NUMERO_DOCUMENTO, 
        a.POBLACION_ESPECIAL,
        a.APELLIDO1, 
        a.APELLIDO2, 
        a.NOMBRE1,
        a.NOMBRE2,
        a.FECHANAC AS FECHA_NACIMIENTO, 
        a.SEXO,
        e.EMBARAZO,
        a.TIPDISCAP AS TIPO_DISCAPACIDAD,
        a.GRDDISCAP AS GRADO_DISCAPACIDAD, 
        a.DIRECRES AS DIRECCION_RECIDENCIA, 
        a.TELEFRES AS TELEFONO_RECIDENCIA,
        a.CODBARES AS CODIGO_BARRIO,
        d.NOMBRE AS COMUNA,
        c.NOMBRE AS BARRIO,
        a.ETNICO, 
        b.NOMBRE AS PAIS, 
        h.NOMBRE, 
        j.NOMBRE AS CUPS, 
        f.ATENCION_FACTURA, 
        f.CODIGO, 
        f.ATENDIDO 
        FROM fac_m_tarjetero a
        JOIN gen_p_paises b ON b.PAIS = a.PAIS 
        JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES 
        JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA 
        JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
        JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
        JOIN fac_p_control g ON g.IPS = f.IPS
        JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD 
        JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
        WHERE h.CODIGO = 1601 AND  a.FECHANAC > CONVERT(datetime, '2011-11-29 12:00:00', 120)
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
        WHERE h.CODIGO = 1601 AND  a.FECHANAC > CONVERT(datetime, '2011-11-29 12:00:00', 120)`
        const count = await sequelize.query(queryCount, {type: QueryTypes.SELECT});
        console.log(count);
        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }
        res.status(200).json({ 
            page,
            totalpage: Math.round(count[0].data / limit),
            itemperpage: limit,
            islast: Math.round(count[0].data / limit) == page,
            total: count[0].data,
            data: resul}) 
        
    } catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
    }
}
