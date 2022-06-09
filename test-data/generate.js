#!/usr/bin/env node

'use strict';

const fs = require('fs');
const util = require('util');

class Request {
    constructor() {
        this.invoiceId = '';
        this.accountId = '';
        this.purchaseTime = 0;
        this.purchaseAmount = 0;
        this.purchaseItemNumber = 0;
        this.location = 0;
    }
}


function main() {
    var recordNumber = 100;
    var fileNumber = 2;

    var invoicecounter = 1000;

    for (var targetFile = 0; targetFile < fileNumber; targetFile++) {

        var message = 'invoiceId,accountId,purchaseTime,purchaseAmount,purchaseItemNumber,location\n';
        for (var targetRecord = 0; targetRecord < recordNumber; targetRecord++) {

            var requestMessage = new Request();
            requestMessage.invoiceId = 'i-' + invoicecounter;
            requestMessage.accountId = 'a-' + (1000 + Math.floor((Math.random() * 10)));
            requestMessage.purchaseTime = Date.now() - Math.floor((Math.random() * 1000));
            requestMessage.purchaseAmount = 10 +  Math.floor((Math.random() * 1000));
            requestMessage.purchaseItemNumber = 1  +  Math.floor((Math.random() * 10));
            requestMessage.location = Math.floor((Math.random() * 100));
            invoicecounter++;

            message = message + requestMessage.invoiceId + ',' +
            requestMessage.accountId + ',' +
            requestMessage.purchaseTime + ',' +
            requestMessage.purchaseAmount + ',' +
            requestMessage.purchaseItemNumber + ',' +
            requestMessage.location + '\n';
        }

        try {
            fs.writeFileSync(util.format('dataset_%d.csv', targetFile), message);
            //file written successfully
        } catch (err) {
            console.error(err)
        }

    }
}

main();