const  jwt = require('jsonwebtoken');

const validarJWT = ( req, res, next ) => {

    const token = req.header('x-token');

    if ( !token ) {
        return res.status( 401 ).json({
            message: 'No hay token en la peticion'
        })
    }

    try {

        const { usuario } = jwt.verify( token, process.env.JWT_SECRET );
        
    } catch (error) {
        console.log( error );
        return res.status( 401 ).json({
            message: 'Token no valido.'
        })
    }

    console.log( token );

    next();
}

module.exports = { validarJWT };