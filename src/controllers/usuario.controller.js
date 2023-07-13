const PERFIL = require("../modelos/perfil.model")
const USUARIO = require("../modelos/usuario.model")
const bcrypt = require('bcryptjs');


exports.listaPerfiles = async (req, res) => {
  
  try {

    const listadoPerfiles = await PERFIL.findAll({});
    if (!listadoPerfiles.length) {

        return res.status(404).json({msg: 'No se Encontraron Perfiles Registrados'});
        
    }

  res.status(200).json({msg: 'Listado de perfiles', data: listadoPerfiles})
  } catch (error) {
    return res.status(500).json({msg: error.message})
  } 

}

exports.crearPerfil = async (req, res) => {

    try {
        const  body  =   req.body;
      
        const validar = await PERFIL.findAll({ where: { nombre: body.nombre }});
  
        if ( validar.length ) {
          return res.status(404).json({
            message: `El perfil ${ body.nombre }, ya se encuentra registrado.`
          })
        }
  
        const Perfil   =   await PERFIL.create({ nombre: body.nombre });
      
        return res.status( 200 ).json({
          message: 'Perfil creado',
          Perfil
        }) 
  
    } catch (error) {
      console.log( error );
      res.status( 500 ).json({
          error   : error,
          message : 'Error inesperado... revisar los logs'
      });
    }
}

exports.listarUsuarios = async (req, res) => {
  
    try {
    const listarUsuarios = await USUARIO.findAll({
        include: { model: PERFIL }
    });
    if (!listarUsuarios.length) {

        return res.status(404).json({msg: 'No se Encontraron Usuarios Registrados'});
        
    }

    res.status(200).json({msg: 'Listado de usuarios', data: listarUsuarios})
    } catch (error) {
    return res.status(500).json({msg: error.message})
    } 
  
}

exports.crearUsuario = async(req, res) => {
    try {

        const body = req.body;
        const salt = bcrypt.genSaltSync();
        let password = bcrypt.hashSync(String(body.cedula), salt)
        console.log(password);
  
        const Usuario = await USUARIO.create({
  
          nombres: body.nombres,
          apellidos: body.apellidos,
          email: body.email,
          id_perfil: body.id_perfil,
          cedula: body.cedula,
          password: password
  
        });
  
        res.status( 200 ).json({
            msg: 'Registro Exitoso',
            Usuario
        }) 
  
    } catch (error) {
      console.log( error );
      res.status( 500 ).json({
          error   : error,
          message : 'Error inesperado... revisar los logs'
      });
    }
}

exports.editarUsuario = async (req, res) => {
  try {
    const { id } = req.params; // Obtener el ID del usuario a editar
    const body = req.body; // Obtener los datos actualizados del usuario

    // Buscar el usuario por ID
    const usuario = await USUARIO.findByPk(id);
    if (!usuario) {
      return res.status(404).json({ msg: 'Usuario no encontrado' });
    }

    if (body.password) {
      const salt = bcrypt.genSaltSync();
      let password = bcrypt.hashSync(String(body.password), salt);
      body.password = password;
    }
    
    const actualizarUsuario = await USUARIO.update(
      body, 
      {
        where: {
          id
        }
      }
    )

    res.status(200).json({ msg: 'Usuario actualizado', data: actualizarUsuario });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      error: error,
      message: 'Error inesperado... revisar los logs',
    });
  }
};