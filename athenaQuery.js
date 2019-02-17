const AWS = require('aws-sdk');
const credentials = new AWS.SharedIniFileCredentials({profile: 'default'});
AWS.config.credentials = credentials;
const athena = new AWS.Athena({
    region: 'us-east-1'
});

/**
 * @description createQueryExecutionId, execute for generating queryExecutionId
 * @param {Function} callback
 */
function createQueryExecutionId(callback){
    /**doing resultConfiguration, but we will not save query result there. */
    const params = {
        QueryString: 'select * From testdatabase.test-table', /* required */
        ResultConfiguration: { /* required */
            OutputLocation: `s3://athena-query-output-log/`, /* required */
            EncryptionConfiguration: {
                EncryptionOption: 'SSE_S3', /* required */
            }
        }
    };
    athena.startQueryExecution(params, function(err, data) {
        callback(err?err.stack:err, data)
    });
}
/**
 * @description checkQueryCreateStatus, check query status till it is not active.
 */
function checkQueryCreateStatus(callback){
    const params = {
        QueryExecutionId: this.QueryExecutionId /* required */
    };
    athena.getQueryExecution(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else{
            if(data && data.QueryExecution && data.QueryExecution.Status && data.QueryExecution.Status.State && data.QueryExecution.Status.State === 'RUNNING'){
                console.log("Athena Query status is running");
                callback("RUNNING");
            }
            else{
                console.log("Atehna Query status is Active")
                callback(err?err.stack:err, data);
            }
        }
    });
}
/**
 * @description getQueryResultByExecutionId, execute for generating result based on queryExecutionId
 * @param {String} queryExecutionId
 * @param {Function} callback
 */
function getQueryResultByExecutionId(queryExecutionId, callback){
    const params = {
        QueryExecutionId: queryExecutionId
    };
    athena.getQueryResults(params, function(err, data) {
        // console.log(err, data)
        callback(err?err.stack:err, data) 
    });
}

/**
 * @description stopQueryExecutionId, execute for stop queryExecutionId
 * @param {String} queryExecutionId
 * @param {Function} callback
 */
function stopQueryExecutionId(queryExecutionId, callback){
    const params = {
        QueryExecutionId: queryExecutionId
    };
    athena.stopQueryExecution(params, function(err, data) {
        callback(err?err.stack:err, data) 
    });
}

module.exports = {
    createQueryExecutionId: createQueryExecutionId,
    checkQueryCreateStatus: checkQueryCreateStatus,
    getQueryResultByExecutionId: getQueryResultByExecutionId,
    stopQueryExecutionId: stopQueryExecutionId
}
