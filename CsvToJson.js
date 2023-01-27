const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const sqs = new AWS.SQS();

exports.handler = async (event) => {
  // Get the S3 bucket and key from the event
  const bucket = event.Records[0].s3.bucket.name;
  const key = event.Records[0].s3.object.key;
  const fileDate = key.substr(key.indexOf("_")); //extract Date from file name

  let fileNames = [
    `clients${fileDate}`,
    `portfolio${fileDate}`,
    `accounts${fileDate}`,
    `transactions${fileDate}`,
  ];

  let filesExist = true; //flag to check if all files exist
  for (const fileName of fileNames) {
    if (fileName === key) continue;
    try {
      // Try to head the object
      await s3.headObject({ Bucket: bucket, Key: fileName }).promise();
    } catch (err) {
      if (err.code === "NotFound") {
        // File does not exist
        filesExist = false;
        console.log(`Object ${fileName} does not exist in ${bucket}`);
        break;
      } else {
        console.log(
          `Error while checking object ${fileName} in ${bucket}: ${err}`
        );
        filesExist = false;
        break;
      }
    }
  }
  if (!filesExist) {
    console.log("Exiting as some files are missing");
    return;
  }

  // S3 Select to query the CSV file and convert it to JSON
  async function getS3Data(S3Query, fileName) {
    const selectParams = {
      Bucket: bucket,
      Key: fileName,
      ExpressionType: "SQL",
      InputSerialization: {
        CSV: {
          FileHeaderInfo: "USE",
          RecordDelimiter: "\n",
        },
      },
      OutputSerialization: {
        JSON: {}, // Request data in JSON format
      },
      Expression: S3Query, //SQL query
    };
    try {
      const selectData = await s3.selectObjectContent(selectParams).promise();
      let jsonString = "";
      selectData.Payload.on("data", (chunk) => {
        jsonString += chunk.toString();
      });
      await new Promise((resolve) => selectData.Payload.on("end", resolve));
      return JSON.parse(jsonString);
    } catch (err) {
      return err;
    }
  }

  // Getting all client_references
  let clientRefs = await getS3Data(
    "SQL Query for getting all client_references",
    `clients${fileDate}`
  );

  //Getting all portfolio_references
  let portfolioRefs = await getS3Data(
    "SQL Query for getting all portfolio_references",
    `portfolios${fileDate}`
  );

  // Going through each client ref and gathering required info to form the client message
  for (const clientRef of clientRefs) {
    try {
      let clientAccounts = await getS3Data(
        `SQL Query all accounts owned by the client using ${clientRef} and searching in portfolios`,
        `portfolios${fileDate}`
      );
      // Forming the client message
      let clientMessage = {
        type: "client_message",
        client_reference: clientRef,
        tax_free_allowance: await getS3Data(
          `SQL Query for tax_free_allowance for specified client using ${clientRef}`,
          `clients${fileDate}`
        ),
        taxes_paid: await getS3Data(
          `SQL Query for sum taxes paid across all accounts held by this client ref by using ${clientAccounts}`,
          `accounts${fileDate}`
        ),
      };

      const sqsParams = {
        MessageBody: JSON.stringify(clientMessage),
        QueueUrl: "SQS_QUEUE_URL",
      };
      await sqs.sendMessage(sqsParams).promise(); //passing client message to SQS
    } catch (err) {
      let errMessage = {
        type: "error_message",
        client_reference: clientRef,
        portfolio_reference: null,
        message: err.code,
      };

      const sqsParams = {
        MessageBody: JSON.stringify(errMessage),
        QueueUrl: "SQS_QUEUE_URL",
      };
      await sqs.sendMessage(sqsParams).promise(); //passing error message to SQS
    }
  }

  // Going through each portfolio ref and gathering required info to form the portfolio message
  for (const portfolioRef of portfolioRefs) {
    try {
      let portfolioAccount = await getS3Data(
        `SQL Query for account number for each account using ${portfolioRef}`,
        `portfolios${fileDate}`
      );
      // Forming the portfolio message
      let portfolioMessage = {
        type: "portfolio_message",
        portfolio_reference: portfolioRef,
        cash_balance: await getS3Data(
          `SQL Query to the accounts file using ${portfolioAccount} and returning the value`,
          `accounts${fileDate}`
        ),
        number_of_transactions: await getS3Data(
          `SQL Query to the transactions file and using ${portfolioAccount} as attribute, returns total count`,
          `transactions${fileDate}`
        ),
        sum_of_deposits: await getS3Data(
          `SQL Query to the transactions file and using ${portfolioAccount} and DEPOSIT keyword as selectors and sum up the amount attr of all results`,
          `transactions${fileDate}`
        ),
      };

      const sqsParams = {
        MessageBody: JSON.stringify(portfolioMessage),
        QueueUrl: "SQS_QUEUE_URL",
      };
      await sqs.sendMessage(sqsParams).promise(); //passing portfolio message to SQS
    } catch (err) {
      let errMessage = {
        type: "error_message",
        client_reference: null,
        portfolio_reference: portfolioRef,
        message: err.code,
      };

      const sqsParams = {
        MessageBody: JSON.stringify(errMessage),
        QueueUrl: "SQS_QUEUE_URL",
      };
      await sqs.sendMessage(sqsParams).promise(); //passing error message to SQS
    }
  }
};
