const AWS = require('aws-sdk');
AWS.config.update({
    region:'ap-south-1'
});
const S3 = new AWS.S3();
const bucketName = 'mediatube';
const fs = require('fs');

const dynamodb = new AWS.DynamoDB.DocumentClient();
const mediaTableName = 'media';

const path = '/media';
const pathS3 = "/media/s3";

exports.handler = async (event) => {
    let response;
    switch (true) {
        case event.httpMethod === 'POST' && event.path === path:
            response = await saveMedia(JSON.parse(event.body));
            break;
        case event.httpMethod === 'GET' && event.path === path+"/all":
            response = await allMedia();
            break;
        case event.httpMethod === 'GET' && event.path === path:
            response = await getMedia(event.queryStringParameters.id);
            break;
        case event.httpMethod === 'PATCH' && event.path === path:
            let input = JSON.parse(event.body)
            response = await modifyMedia(input.id,input.updateKey,input.updateValue);
            break;
        case event.httpMethod === 'DELETE' && event.path === path:
            response = await deleteMedia(event.queryStringParameters.id);
            break;
        case event.httpMethod === 'GET' && event.path === pathS3:
            response = await getFileStream(event.queryStringParameters.key);
            break;
        case event.httpMethod === 'POST' && event.path === pathS3:
            response = await uploadFileS3();
            break;
        default:
            response = buildResponse(404, '404 Notst Found');
    }
    return response;
};


async function saveMedia(data){
    let date = new Date();
    let createInput = {
        "id":AWS.util.uuid.v4(),
        "user_id":(data?.user_id)?data.user_id:"",
        "category" :(data?.category)?data.category:[],
        "title":(data?.title)?data.title:"",
        "thumbnail":(data?.thumbnail)?data.thumbnail:"",
        "media_path":(data?.media_path)?data.media_path:"",
        "media_type":(data?.media_type)?data.media_type:"",
        "view":[],
        "like":[],
        "unlike":[],
        "comments":[],
        "createdAt":date,
        "updatedAt":date,
    }
    const params = {
        TableName: mediaTableName,
        Item : createInput
    }
    return await dynamodb.put(params).promise().then(() =>{
        const body ={
            Operation:'SAVE',
            Message: 'SUCCESS',
            Item:createInput
        }
        return buildResponse(200,body);
    },(error)=>{
        console.error('Error in saveMedia',error);
    })
    
    
}

async function allMedia(){
    const params ={
        TableName : mediaTableName
    }
    const allMedias = await scanDynamoRecords(params,[]);
    const body = {
        media: allMedias
    }
    return buildResponse(200,body);
}

async function scanDynamoRecords(scanParams, itemArray) {
  try {
    const dynamoData = await dynamodb.scan(scanParams).promise();
    itemArray = itemArray.concat(dynamoData.Items);
    if (dynamoData.LastEvaluatedKey) {
      scanParams.ExclusiveStartkey = dynamoData.LastEvaluatedKey;
      return await scanDynamoRecords(scanParams, itemArray);
    }
    return itemArray;
  } catch(error) {
    console.error('Do your custom error handling here. I am just gonna log it: ', error);
  }
}

async function getMedia(id) {
  const params = {
    TableName: mediaTableName,
    Key: {
      'id': id
    }
  }
  return await dynamodb.get(params).promise().then((response) => {
    return buildResponse(200, response.Item);
  }, (error) => {
    console.error('Do your custom error handling here. I am just gonna log it: ', error);
  });
}


async function modifyMedia(id, updateKey, updateValue) {
  const params = {
    TableName: mediaTableName,
    Key: {
      'id': id
    },
    UpdateExpression: `set ${updateKey} = :value`,
    ExpressionAttributeValues: {
      ':value': updateValue
    },
    ReturnValues: 'UPDATED_NEW'
  }
  return await dynamodb.update(params).promise().then((response) => {
    const body = {
      Operation: 'UPDATE',
      Message: 'SUCCESS',
      UpdatedAttributes: response
    }
    return buildResponse(200, body);
  }, (error) => {
    console.error('Do your custom error handling here. I am just gonna log it: ', error);
  })
}

async function uploadFileS3() {
  try {
      let input = {
        path:'./media/data.txt',
        fileName: "text-"+ new Date().getTime()+".txt"
      }
      let result = await uploadToS3(input);
      return buildResponse(200, result);
    } catch(error) {
      console.error('Error in upload s3', error);
    }
}

async function uploadToS3 (data){
     let fileStream = await fs.createReadStream(data.path);
    const uploadParams = {
        Bucket: bucketName,
        Body: fileStream,
        Key: data.fileName,
        ACL:'public-read-write',
    }
    return await S3.upload(uploadParams).promise()
}
async function deleteMedia(id) {
  const params = {
    TableName: mediaTableName,
    Key: {
      'id': id
    },
    ReturnValues: 'ALL_OLD'
  }
  return await dynamodb.delete(params).promise().then((response) => {
    const body = {
      Operation: 'DELETE',
      Message: 'SUCCESS',
      Item: response
    }
    return buildResponse(200, body);
  }, (error) => {
    console.error('Do your custom error handling here. I am just gonna log it: ', error);
  })
}


async function uploadS3(event) {
    try {
        console.log(event);
        const parser = require("lambda-multipart-parser");
        const result = await parser.parse(event);
        const { content, filename, contentType } = result.files.media;
        
        const params = {
            Bucket: bucketName,
            Key: filename,
            Body: content,
            ContentDisposition: `attachment; filename="${filename}";`,
            ContentType: contentType,
            ACL: "public-read"
            };

         return buildResponse(200, params);
      } catch(error) {
        console.error('Error in upload s3', error);
      }
  }

function getFileStream(fileKey) {
    const downloadParams = {
        Key: fileKey,
        Bucket: bucketName
    }

     let result =  S3.getObject(downloadParams).promise();
    return buildResponse('200',result);
}

function buildResponse(statusCode, body) {
  return {
    statusCode: statusCode,
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  }
}

