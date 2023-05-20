const { QueryTypes, and } = require('sequelize');
const {sequelize} = require('../BD-RFAS8/conexiondb');


exports.servicios = async (req, res) => {
    try {
        const queryServicios = `SELECT
        COUNT(*) data,
        h.NOMBRE,
        h.CODIGO
        FROM fac_m_tarjetero a
        JOIN gen_p_paises b ON b.PAIS = a.PAIS
        JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES
        JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA
        JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
        JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
        JOIN fac_p_control g ON g.IPS = f.IPS
        JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD
        JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
        WHERE h.CODIGO = 1607 
        GROUP BY h.NOMBRE, h.CODIGO`
        const resul = await sequelize.query(queryServicios, {type: QueryTypes.SELECT});
        console.log(resul);

        const queryAtendidos = `SELECT
        COUNT(*) atendidos,
        h.NOMBRE,
        h.CODIGO
        FROM fac_m_tarjetero a
        JOIN gen_p_paises b ON b.PAIS = a.PAIS
        JOIN fac_p_barrio c ON c.CODIGO = a.CODBARES
        JOIN fac_p_comuna d ON d.CODIGO = c.COMUNA
        JOIN fac_m_factura e ON e.HISTORIA = a.HISTORIA
        JOIN fac_m_citas f ON f.HISTORIA = e.HISTORIA
        JOIN fac_p_control g ON g.IPS = f.IPS
        JOIN fac_p_centroproduccion h ON h.CODIGO = f.CENTROPROD
        JOIN fac_p_cups j ON j.CODIGO = f.CODIGO_CUPS
        WHERE h.CODIGO = 1607 AND f.ATENDIDO = 1
        GROUP BY h.NOMBRE, h.CODIGO`
        const atendidos = await sequelize.query(queryAtendidos, {type: QueryTypes.SELECT});
        console.log(atendidos);

        if (!resul.length) {
            return res.status(404).json({ msg: 'No se encontraron registros'})
        }

        res.status(200).json({ 
            data: resul,
            atendidos: atendidos
        })

    } catch (error) {
        console.log(error)
        return res.status(500).send('Error en el servidor')
    }
}