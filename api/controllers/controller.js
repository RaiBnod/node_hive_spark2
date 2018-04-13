'use strict';
var connector = require('../../connector');

exports.home = function(req, res) {
  res.json('Home Page Content');
};

exports.list_tables = function(req, res) {
  connector('SHOW TABLES', (err, httpStatusCode, result) => {
    res.status(httpStatusCode).json(result)
  });
};

exports.list_all_acu_sts_n_enb_v1 = function (req, res) {
  connector('SELECT * FROM acu_sts_n_enb_v1', (err, httpStatusCode, result) => {
    res.status(httpStatusCode).json(result);
  });
};

exports.list_a_acu_sts_n_enb_v1 = function (req, res) {
  connector('SELECT * FROM acu_sts_n_enb_v1 WHERE timestamp=' + req.params.timestamp, (err, httpStatusCode, result) => {
    console.log("SELECT * FROM acu_sts_n_enb_v1 WHERE timestamp='" + req.params.timestamp + "'");
    res.status(httpStatusCode).json(result);
  });
};