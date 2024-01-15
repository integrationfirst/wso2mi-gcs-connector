## putObject

The method writes an object to Object Storage with specific metadata.

__Input Parameters__

| Name        | Description                                                                             |
|-------------|-----------------------------------------------------------------------------------------|
| `pojectId`  | The ID of the Google Cloud project                                                      
| `bucket`    | The bucket in which the object will be put. E.g.: `projectA`                            
| `objectKey` | Absolute path to the object, it's without the bucket. E.g.: `/input/2023/12/myfile.jpg` 
| `content`   | The object data. It could be text-based or binary object.                               

__Output Parameters__

| Name              | Description                                               |
|-------------------|-----------------------------------------------------------|
| `putObjectResult` | Indicate the operation result. Value is `true` or `false` 

## getObject

Read the byte array of the object from Object Storage by its `bucket` and `key`

__Input Parameters__

| Name        | Description                                                                             |
|-------------|-----------------------------------------------------------------------------------------|
| `pojectId`  | The ID of the Google Cloud project                                                      
| `bucket`    | The bucket in which the object will be put. E.g.: `projectA`                            
| `objectKey` | Absolute path to the object, it's without the bucket. E.g.: `/input/2023/12/myfile.jpg` 

__Output Parameters__

The output will be put into message body with two separated data fields:

| Name              | Description                                               |
|-------------------|-----------------------------------------------------------|
| `getObjectResult` | Indicate the operation result. Value is `true` or `false` 
| `binaryObject`    | Contains the base64 encoded byte array of the object      

The output message payload would be as below:

```xml

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body xmlns:axis2ns1="http://ws.apache.org/commons/ns/payload">
        <axis2ns1:getObjectResult>true</axis2ns1:getObjectResult>
        <axis2ns1:binaryObject>JVBERi0xLjQKJYCAgIANM...</axis2ns1:binaryObject>
    </soapenv:Body>
</soapenv:Envelope>
```