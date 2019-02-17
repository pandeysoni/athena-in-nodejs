const AthenaQuery = require('./athenaQuery');
const async = require('async');

async.waterfall([
        (callback)=>{
            AthenaQuery.createQueryExecutionId(callback);
        },
        (query, callback)=>{
            async.retry({
                times: 60,
                interval: 1000
            }, AthenaQuery.checkQueryCreateStatus.bind(query), (err, result) => {
                if (!err) {
                   callback(null, query)
                }
                else{
                    callback(err)
                }
            });
        },
        (query, callback)=>{
            AthenaQuery.getQueryResultByExecutionId(query.QueryExecutionId, (err, result)=>{
                callback(null, result, query)
            })
        },
        (queryResult, query, callback)=>{
            AthenaQuery.stopQueryExecutionId(query.QueryExecutionId, (err, result)=>{
                callback(err, queryResult)
            });
        }
        ],(error, result) => {
        	console.log(error, result);
        })