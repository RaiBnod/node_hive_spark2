'use strict';
module.exports = function(app) {
  var test = require('../controllers/controller');

  app.route('/').get(test.home);

  app.route('/list_table').get(test.list_tables);

  app.route('/list_all_acu_sts_n_enb_v1').get(test.list_all_acu_sts_n_enb_v1);

  app.route('/list_a_acu_sts_n_enb_v1/:timestamp').get(test.list_a_acu_sts_n_enb_v1);

};