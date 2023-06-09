const { generarJWT } = require("../auxiliar/jwt");
const USUARIO = require("../modelos/usuario.model");
const PERFIL= require("../modelos/perfil.model");
const bcrypt = require('bcryptjs');
const { token } = require("morgan");


exports.login = async ( req, res ) => {
    try {
        const { email, password } = req.body;

        const Usuario = await USUARIO.findAll({ 
            where: { email: email },
            include: [{ model: PERFIL }],
        });

        if ( !Usuario.length ) {
            return res.status( 404 ).json({
                message: `El correo ${ email } no existe`
            });
        }

        const Password = bcrypt.compareSync( password, Usuario[0].password );
        console.log(Password, password, Usuario[0].password );

        if ( !Password ) {
            return res.status( 404 ).json({
                message: 'Contraseña invalida'
            });
        }

        const usuarioLogeado = { 
            id:  Usuario[0].id,
            perfil: Usuario[0].perfil.dataValues.nombre,
            id_perfil: Usuario[0].perfil.dataValues.id_perfil,
        }
        console.log(Usuario[0].perfil.dataValues);
        const token = await generarJWT( usuarioLogeado.id );

        res.status( 200).json({
            token,
            usuarioLogeado,
            message: 'Operacion exitosa'
        });
        
    } catch ( error ) {
        console.log( error);
        res.status( 500 ).json({
            message: 'Error en el servidor....'
        });
    }
}
exports.validarToken = async(req, res) => {
    try {
        let token = await generarJWT(req.id);
        res.status( 200).json({
            token,
            message: 'Operacion exitosa'
        });
    } catch (error) {
        return res.status(500).json({message: 'Error en el servidor...'})
    }
}