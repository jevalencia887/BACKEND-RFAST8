const { QueryTypes, and } = require('sequelize');
const {sequelize} = require('../BD-RFAS8/cursovidadb.js');

exports.listaUsuarios = async (req, res) => {
    try {

        res.status(200).json({ 
            usuarios: 'ok'
        })
        
    } catch (error) {
        console.log(error);
        return res.status(500).send('Error en el servidor') 
    }
}
