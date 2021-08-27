const mysql = require('promise-mysql')

const mysqlConfig = {
	user: "ColivHQ-console",
	password: "dsfjhsd7sd8s76sa8JHzdfgUIU7sfzagIUU78",
	database: "colivhq",
	host: "34.87.5.144"
	// socketPath: "/cloudsql/colivhq"
}


let connection = (async function () {
	console.log("-------here-----")
	return mysql.createPool(mysqlConfig)
})()

module.exports = {
	connection,
}