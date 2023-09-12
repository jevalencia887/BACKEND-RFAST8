const PERFIL = require("../modelos/perfil.model")
const USUARIO = require("../modelos/usuario.model")
const bcrypt = require('bcryptjs');
const PERMISO = require("../modelos/permiso.model");
const UsuarioPermiso = require("../modelos/usuario-permiso.model");


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
        include: [{ model: PERFIL }, {model: PERMISO}]
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
      console.log(req.body);
        const body = req.body;
        const salt = bcrypt.genSaltSync();
        let password = bcrypt.hashSync(String(body.cedula), salt)
        
  
        const Usuario = await USUARIO.create({
  
          nombres: body.nombres,
          apellidos: body.apellidos,
          email: body.email,
          id_perfil: body.id_perfil,
          cedula: body.cedula,
          password: password,
          estado: true
  
        });

        await UsuarioPermiso.bulkCreate(
          body.permisos.map( (id_permiso) => ({
            usuarioId: Usuario.id,
            permisoId: id_permiso
          }))
        )
  
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
    );
     
    if (body.permisos.length) {
      console.log(body.permisos);
      // Primero, eliminamos los permisos existentes del usuario
      await UsuarioPermiso.destroy({
        where: {
          usuarioId: id
        }
      });
    
      // Luego, creamos los nuevos registros de permisos
      const uniquePermisos = [...new Set(body.permisos)]; // Eliminar duplicados
      await UsuarioPermiso.bulkCreate(
        uniquePermisos.map(id_permiso => ({
          usuarioId: id,
          permisoId: id_permiso
        }))
      );
    }
    
    

    res.status(200).json({ msg: 'Usuario actualizado', data: actualizarUsuario });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      error: error,
      message: 'Error inesperado... revisar los logs',
    });
  }
};

exports.actualizarEstadoUsuario = async (req, res) => {
  try {
    const { id } = req.params; // Obtener el ID del usuario a actualizar el estado
    const { estado } = req.body; // Obtener el nuevo estado del usuario

    // Buscar el usuario por ID
    const usuario = await USUARIO.findByPk(id);
    if (!usuario) {
      return res.status(404).json({ msg: 'Usuario no encontrado' });
    }

    await USUARIO.update(
      { estado },
      {
        where: {
          id,
        },
      }
    );

    res.status(200).json({ msg: 'Estado de usuario actualizado' });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      error: error,
      message: 'Error inesperado... revisar los logs',
    });
  }
};

exports.editarPermisos = async (req, res) => {
  try {
    const { id } = req.params; // Obtener el ID del usuario a editar

    // Buscar el usuario por ID
    const usuario = await USUARIO.findByPk(id);
    if (!usuario) {
      return res.status(404).json({ msg: 'Usuario no encontrado' });
    }
    
    const actualizarPermisos = await UsuarioPermiso.update(
      body, 
      {
        where: {
          id
        }
      }
    )

    res.status(200).json({ msg: 'Usuario actualizado', data: actualizarPermisos });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      error: error,
      message: 'Error inesperado... revisar los logs',
    });
  }
};

exports.listaPermisos = async (req, res) => {
  
  try {

    const listadoPermisos = await PERMISO.findAll({});
    if (!listadoPermisos.length) {

        return res.status(404).json({msg: 'No se Encontraron Permisos Registrados'});
        
    }

  res.status(200).json({msg: 'Listado de Permisos', data: listadoPermisos})
  } catch (error) {
    return res.status(500).json({msg: error.message})
  } 

}

