// bird tracking
// ethan trott
// 2022

var https = require("https");
var fs = require("fs");

const schedule = require('node-schedule');

const sql = require('./sql-interact');

var config = require("./config.json");

function refreshToken(){
    const options = {
        method: 'POST',
        hostname: 'api-auth.prod.birdapp.com',
        port: 443,
        path: '/api/v1/auth/refresh/token',
        headers: {
            "User-Agent": "Bird/4.119.0(co.bird.Ride; build:3; iOS 14.3.0) Alamofire/5.2.2",
            "Device-Id": config.GUID,
            "Platform": "ios",
            "App-Version": "4.119.0",
	    "App-Name": "Bird",
            "Authorization": "Bearer "+config.refreshToken
        }
    }

    var post_req = https.request(options, (response) => {
        //response.setEncoding('utf8');
        var result = ''
        response.on('data', function (chunk) {
            result += chunk;
        });

        response.on('end', function () {
            try {
                //get new credentials, write to config
                var resData = JSON.parse(result);
                config.accessToken = resData.access;
                config.refreshToken = resData.refresh;

                //save updated config file
                fs.writeFile("./config.json", JSON.stringify(config), function (err) {
                    if (err) throw err;
                });

                console.log("Tokens successfully refreshed.");
            }
            catch(e){ console.log(e) }
        });

        response.on('error', function (error) {
            console.log("token refresh error", error);
        });
    });

    try{
        console.log("Attempting token refresh...");
        post_req.write("");
        post_req.end();
    } catch (e) { console.log(e) }
}

function getBirds(){
    const queryString = '/bird/nearby?latitude='+config.location.latitude+'&longitude='+config.location.longitude+'&radius='+config.radius;

    const options = {
        hostname: 'api-bird.prod.birdapp.com',
        path: queryString,
        headers: {
            "User-Agent": "Bird/4.119.0(co.bird.Ride; build:3; iOS 14.3.0) Alamofire/5.2.2",
            "Device-Id": config.GUID,
            "Platform": "ios",
            "App-Version": "4.119.0",
	    "App-Name": "Bird",
            "Authorization": "Bearer "+config.accessToken,
            "Location": JSON.stringify(config.location)
        }
    }

    https.get(options, (response) => {
        var result = ''
        response.on('data', function (chunk) {
            result += chunk;
        });

        response.on('end', function () {
            const date = new Date().toISOString();
            console.log("Processing: "+date);
            var fileSavePath = "data/"+date+".json";
            fs.writeFile(fileSavePath, result, function (err) {
                if (err) throw err;
            });
            console.log("Saved file.");

            sql.uploadToDB(result, date);
        });

        response.on('error', function (error) {
            console.log("bird fetch error", error);
        });
    });
}

//every minute, get bird data and log it 
const dataLogJob = schedule.scheduleJob('0 * * * * *', getBirds);

//every 6 hours refresh auth token
const refreshJob = schedule.scheduleJob('30 0 */6 * * *', refreshToken);

//ignore network errors
process.on('uncaughtException', (err) => console.log('Process Error: ', err));

console.log("Running :)");