const login = require("./module/login");
// Support all method? :v
module.exports = login;
Object.assign(module.exports, { login, default: login });