const { generarJWT } = require("../auxiliar/jwt");
const USUARIO = require("../modelos/usuario.model");
const PERFIL= require("../modelos/perfil.model");
const bcrypt = require('bcryptjs');
const { token } = require("morgan");
const { Op } = require("sequelize");
const PERMISO = require("../modelos/permiso.model");


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

        const validarUsuario = await USUARIO.findAll({ 
            where: { 
                [Op.and]: [
                    {email},
                    {estado: true}
                ]
            },
            include: [{ model: PERFIL }],
        });

        const Password = bcrypt.compareSync( password, Usuario[0].password );
        

        if ( !Password ) {
            return res.status( 404 ).json({
                message: 'Contraseña invalida'
            });
        }
        if ( !validarUsuario.length ) {
            return res.status( 404 ).json({
                message: `El usuario esta inavilitado`
            });
        }
        const usuarioLogeado = { 
            id:  Usuario[0].id,
            perfil: Usuario[0].perfil.dataValues.nombre,
            id_perfil: Usuario[0].perfil.dataValues.id_perfil,
        }

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

        let token = await generarJWT( req.id );
        

        const id = req.params.id;

        const usuarioLogeado = await USUARIO.findAll({ 
            where: { 
                [Op.and]: [
                    { estado: true },
                    { id: id }
                ]
            },
            include: [{ model: PERMISO }],
        });

        res.status( 200).json({
            token,
            message: 'Operacion exitosa',
            usuarioLogeado
        });
    } catch (error) {
        console.log( error )
        return res.status(500).json({message: 'Error en el servidor...'})
    }
}
