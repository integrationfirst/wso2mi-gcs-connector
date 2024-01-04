### putObject

__Input Message__

The method requires an input envelop contains _only one node_ with binary data in it.

For example:

```xml

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <axis2ns1:binary xmlns:axis2ns1="http://ws.apache.org/commons/ns/payload">JVBERi0xLjQKJYCAgIANM...
        </axis2ns1:binary>
    </soapenv:Body>
</soapenv:Envelope>
```

__Input Parameters__

| Name         | Description                                                                             |
|--------------|-----------------------------------------------------------------------------------------|
| address      | Endpoint of the object storage. E.g.: https://myoss.com                                 
| bucket       | The bucket in which the object will be put. E.g.: `projectA`                            
| objectKey    | Absolute path to the object, it's without the bucket. E.g.: `/input/2023/12/myfile.jpg` 
| accessKey    | The key for authentication                                                              
| accessSecret | The secret for authentication                                                           

__Output Parameters__

| Name            | Description                |
|-----------------|----------------------------|
| putObjectResult | One of `SUCCESS` or `null` 


