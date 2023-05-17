const jwt = require('jsonwebtoken')

const generarJWT = ( usuario ) => {

    return new Promise( ( resolve, reject ) => {
        const payload = {
           usuario
            
        };
    
        jwt.sign( payload, process.env.JWT_SECRET, {
            expiresIn: '24h'

        }, ( error, token ) => {
            if ( error ) {
                console.log( error );
                reject('No se pudo generar token');
            } else {
                resolve( token );
            }
        })

    });
}

module.exports = { generarJWT };