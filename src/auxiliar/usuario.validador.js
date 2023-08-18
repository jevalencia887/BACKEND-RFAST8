const { check } = require("express-validator")
const { validarCorreo, validarCedula, validarPerfil } = require("./validacion-db")
const { validateResult } = require("../middlewares/validator")


const validarUsuario = [ 


    check('nombres'   , 'El nombre es obligatorio')
        .exists ()
        .not    ()
        .isEmpty()
        .isString(),

    check('apellidos', 'Los apellidos son obligatorios')
        .exists ()
        .not    ()
        .isEmpty()
        .isString(),
    
    check('email', 'El correo no tiene el formato correcto')
        .exists ()
        .not    ()
        .isEmpty()
        .isString()
        .isEmail(),

    check('email').custom( ( email ) => validarCorreo( email ) ),

    check('cedula', 'La cedula es obligatoria')
        .exists ()
        .not    ()
        .isEmpty()
        .isInt(),
        
    check('cedula').custom( ( cedula ) => validarCedula( cedula ) ),

    
    check('id_perfil', 'El perfil es obligatorio')
        .exists ()
        .not    ()
        .isEmpty()
        .isInt(),

    check('id_perfil').custom( ( id_perfil ) => validarPerfil( id_perfil ) ),

(req, res, next) => {
    validateResult(req, res, next)
}
]

module.exports = {validarUsuario}