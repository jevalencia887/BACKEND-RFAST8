const PERFIL = require("../modelos/perfil.model");
const USUARIO = require("../modelos/usuario.model");


const validarCorreo = async ( email = '' ) => {
    const validar = await USUARIO.findAll({
        where: { email: email },
      });

      if ( validar.length ) {
          throw new Error(`El correo ${ email }, ya esta registrado`)
      } 
}

const validarCedula = async ( cedula = '' ) => {
    const validar = await USUARIO.findAll({
        where: { cedula: cedula },
      });

      if ( validar.length ) {
          throw new Error(`La cedula ${ cedula }, ya esta registrada`)
      } 
}

const validarPerfil = async ( id_perfil = '' ) => {
    const validar = await PERFIL.findByPk( id_perfil );

      if ( !validar ) {
          throw new Error('El perfil no existe')
      } 
}

module.exports = {validarCedula, validarCorreo, validarPerfil}