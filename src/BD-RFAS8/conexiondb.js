const { Sequelize } = require("sequelize");

exports.sequelize = new Sequelize(process.env.DB_NAME, process.env.DB_USERNAME, process.env.DB_PASSWORD, {
    dialect: 'mssql',
    host: process.env.DB_SERVER,
    dialectOptions: {
        requestTimeout: 90000000,
        options: {
        "encrypt": false,
        enableArithAbort: true,
        requestTimeout: 90000000,
        idleTimeoutMillis: 90000000
        },
            pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 90000000
        }   
    },
});

exports.sequelize1 = new Sequelize(process.env.DB_NAME_SQL, process.env.DB_USERNAME_SQL, process.env.DB_PASSWORD_SQL, {
    dialect: 'mysql',
    host: process.env.DB_SERVER_SQL,
    logging: false,
    dialectOptions: {
        requestTimeout: 90000000,
        options: {
        "encrypt": false,
        enableArithAbort: true,
        requestTimeout: 90000000,
        idleTimeoutMillis: 90000000
        },
            pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 90000000
        }   
    },
});

