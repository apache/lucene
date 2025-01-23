# aws-lightweight-client-java
<a href="https://github.com/davidmoten/aws-lightweight-client-java/actions/workflows/ci.yml"><img src="https://github.com/davidmoten/aws-lightweight-client-java/actions/workflows/ci.yml/badge.svg"/></a><br/>
[![codecov](https://codecov.io/gh/davidmoten/aws-lightweight-client-java/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/aws-lightweight-client-java)<br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/aws-lightweight-client-java/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/aws-lightweight-client-java)<br/>

This is a really lightweight standalone artifact (about 75K) that performs authentication (signing requests with AWS Signature Version 4) and helps you build requests against the AWS API. It includes nice concise builders, a lightweight inbuilt xml parser (to parse responses), an xml builder, and useful convenience methods.

Aside from cold-start runtime improvements in AWS Lambda, the small artifact size is presumably attractive for mobile device developers (Android especially).

**Features**
* small standalone artifact (75K)
* concise fluent api
* signs requests with AWS Signature Version 4
* generates presigned urls
* supports throwing custom exceptions
* metadata and attributes support
* xml response parsing support
* xml builder
* 100% unit test coverage
* reduces average Lambda cold start time significantly
* S3 Multipart upload [helper](https://github.com/davidmoten/aws-lightweight-client-java/wiki/Recipes#multipart-upload-a-file)

**Status**: released to [Maven Central](https://search.maven.org/artifact/com.github.davidmoten/aws-lightweight-client-java)

Maven [reports](https://davidmoten.github.io/aws-lightweight-client-java/index.html) including [javadocs](https://davidmoten.github.io/aws-lightweight-client-java/apidocs/index.html)

For example with the 75K standalone artifact you can download an object from an S3 bucket:

```java
Client s3 = Client.s3()
  .region("ap-southeast-2")
  .accessKey(accessKey)
  .secretKey(secretKey)
  .build();

String content = s3
  .path("myBucket", "myObject.txt")
  .responseAsUtf8();
```

Here's how to create an SQS queue and send a message to that queue. This time we'll create our Client for use in a Lambda handler (credentials are picked up from environment variables):
```java
Client sqs = Client.sqs().defaultClient().build();

String queueUrl = sqs
    .query("Action", "CreateQueue")
    .query("QueueName", queueName(applicationName, key))
    .responseAsXml()
    .content("CreateQueueResult", "QueueUrl");

sqs.url(queueUrl)
    .query("Action", "SendMessage")
    .query("MessageBody", "hi there")
    .execute();
```

Here's how to upload a file to an S3 bucket using multipart:
```java
Multipart
  .s3(s3)
  .bucket("mybucket")
  .key("mykey")
  .upload(file);
```

See [Recipes](https://github.com/davidmoten/aws-lightweight-client-java/wiki/Recipes) for many more examples.

## Lambda performance
You can see that usage is still pretty concise compared to using the AWS SDK v1 or v2 for Java. There's a significant advantage in using the lightweight client in a Java Lambda.

The test Lambda that I used does this:
* puts a 240B object into an S3 bucket with metadata
* creates an SQS queue
* sends the queue a small message (16 bytes).

Using AWS SDK v1 the shaded minimized jar deployed to Lambda is 5.1MB (7.2MB unminimized), with AWS SDK v2 unminimized jar is 6.9MB (couldn't figure out the shade rules to minimize!) and with *aws-lightweight-client-java* the jar is 80K.

The conclusion from the comparison is that with this scenario Lambdas using *aws-lightweight-client* run their cold-start on average in **40% of the time** as using AWS SDK v1, **45% of the time** as using AWS SDK v2. Not only that but there does seem be a minor advantage in warm runtime (~10% faster).

<img width="500" src="src/docs/graph.jpeg"/>

Here are the comparison details:

I took the AWS SDK v1 and Lightweight lambdas and tested them with different memory allocations. The configured memory also affects the CPU allocation. At 2GB memory a full VCPU is allocated and CPU allocation is proportional to memory allocation.

**Cold Start Runtimes (average)**

| Memory | SDK v1 | Lightweight |
|--------|-----|-------------|
| 128MB  | Metaspace error | 19s |
| 256MB  | 21s             | 8.1s |
| 512MB  | 10.5s           | 3.9s |
| 2GB    | 2.8s           | 1.0s |

**Warm Runtimes (average)**

| Memory | SDK v1 | Lightweight |
|--------|-----|-------------|
| 128MB  | Metaspace error | 2.4s |
| 256MB  | 0.6s             | 0.5s |
| 512MB  | 0.3s           | 0.3s |
| 2GB    | 0.1s           | 0.1s |

Except for the 2GB case I measured cold-start runtimes several times and then 5-10 or so warm runtimes for each case. Much more data was gathered for the 2GB case below.

Note that for AWS SDK v2 I followed the coding recommendations of https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/lambda-optimize-starttime.html. Once exception to the AWS advice is that client objects were created in the handler method rather than instantiated as static fields.

**Lambda runtimes for 2GB Memory in seconds**

|          | SDK v1 Cold| SDK v2 Cold |Lightweight Cold| SDK v1 Warm | SDK v2 Warm |Lightweight Warm |
|----------|--------|------|-------|-------|-----|-----|
| average | 2.772 | 2.289 |1.04 |0.116 |0.130| 0.101|
| stdev   | 0.448 | 0.130 | 0.116 |0.017|0.016| 0.014|
| max     | 4.315 | 2.941 |1.30 | ? | ? | ?|
| min     | 2.471 | 1.976 | 0.91 | 0.057 | 0.068 | 0.048 |
| samples | 24 | 30 | 25 | 216 | 270 | 225 |

Note that testing shows that using *com.amazonaws:aws-java-sdk-s3:1.11.1032* getting an object from an S3 bucket requires loading of 4203 classes yet using *aws-lightweight-client-java:0.1.3* requires loading of 2350 classes (56%). Using the AWS SDK v2 *software.amazon.awssdk:s3:2.16.78* still uses 3639 classes.

### Instantiating client objects as static fields
One optimization suggested by AWS advice is to instantiate client objects (like `AwsS3Client`) in static fields so that the creation of the handler object brings about the once-only instantiation of the client objects. This doesn't necessarily have much of an effect on cold start time in terms of the total cold-start request time to the lambda but it does affect the billable runtime (it reduces it a lot). AWS charges for the runtime of the handler method call and the instantiation of the handler object is not part of that. Thus the reasonably lengthy period of class loading that happens on instantiation of the client objects is associated with the initialization phase of the lambda and is outside the billable runtime.

I ran three lambdas once an hour (cold-start) and 10 times in succession immediately after the cold-start (warm invocations) and gathered some stats over a 28+ hour period. The source code for the three lambdas are below (everything is there for the full integration including cloudformation.yaml and deployment scripts):

* AWS SDK v1 [handler](https://github.com/davidmoten/one-time-link-aws/blob/f3a11547c187216e2e1477d27726a3432348a73a/src/main/java/com/github/davidmoten/onetimelink/lambda/Handler.java) (`store` resource path)
* AWS SDK v2 [handler](https://github.com/davidmoten/one-time-link-aws/blob/891e09ecc2c4019d00c33993be39098b8e91bfc4/src/main/java/com/github/davidmoten/onetimelink/lambda/Handler.java) (`store` resource path)
* Lightweight client [handler](https://github.com/davidmoten/one-time-link-aws/blob/1.0.19/src/main/java/com/github/davidmoten/onetimelink/lambda/Handler.java) (`store` resource path)

When you want to gather some statistics about the initialization phase as well as the billable runtime then you need to enable trace logging and the AWS XRay service to explore them. Unfortunately mucking about with XRay and trace logging is a bit painful when you want to look at longer than 6 hours so I've opted for another approach where I simply measure the full response time for an API Gateway + Lambda integration (I did leave trace logging and xray enabled though for analysis later if I get around to it).

**Cold start request times (seconds) API Gateway + Lambda 2GB Memory**

| | Average | Stdev | Min | Max | n |
|-------|-------|-------|------|-------|------|
| **AWS SDK v1** | 3.987 | 0.320 | 3.583 | 5.280 | 28 |
| **AWS SDK v2** | 3.153 | 0.267 | 2.918 | 4.060 | 28 |
| **lightweight** | 1.938 | 0.149 | 1.739 | 2.376 | 28 |

Note that these requests were made from my not-very-snappy home internet connection. The deltas are informative though given all requests had the same payload and response body.

TODO warm invocation analysis

## Getting started
Add this dependency to your pom.xml:

```xml
<dependency>
    <groupId>com.github.davidmoten</groupId>
    <artifactId>aws-lightweight-client-java</artifactId>
    <version>VERSION_HERE</version>
</dependency>
```

## Usage

To perform actions against the API you do need to know what methods exist and the parameters for those methods. This library is lightweight because it doesn't include a mass of generated classes from the API so you'll need to check the AWS API documentation to get that information. For example the API docs for S3 is [here](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).

### Creating a Client
In a Lambda handler environment variables hold the credentials and session token. To pick those values up:

```java
Client s3 = Client.s3().defaultClient().build();
```
Outside of lambda you might specify your credentials explicitly:

```java
Client s3 = Client
  .s3()
  .region("ap-southeast-2")
  .accessKey(accessKey)
  .secretKey(secretKey)
  .build()
```
There are a number of other options that can be set when building the Client:

```java
Client iam = Client
  .serviceName("iam")
  .region(region)
  .accessKey(accessKey)
  .secretKey(secretKey)
  .exceptionFactory(myExceptionFactory)
  .exception(
      x -> !x.isOk() && x.contentUtf8().contains("NonExistentPolicy"),
      x -> new PolicyDoesNotExistException(x.contentUtf8()))
  .httpClient(myHttpClient)
  .baseUrlFactory((service, region) -> "https://me.com/")
  .connectTimeout(30000, TimeUnit.MILLISECONDS)
  .readTimeout(120000, TimeUnit.MILLISECONDS)
  .build();
```
A client can be copied from another client to pick up same configuration (but with a different service name):

```java
Client sqs = Client.from(iam).build();
```
### Timeouts
Timeouts can be set in the client builder and also for each request. Here's an example:

```java
Client s3 = Client
  .s3()
  .defaultClient()
  .connectTimeout(30, TimeUnit.SECONDS)
  .readTimeout(60, TimeUnit.SECONDS)
  .build();

 String content = s3
  .path("myBucket", "myObject.txt")
  .connectTimeout(5, TimeUnit.SECONDS)
  .readTimeout(5, TimeUnit.SECONDS)
  .responseAsUtf8();
```
### Retries
Automatic retries can be configured in the client builder and also for each request including multipart requests. Capped
exponential backoff is supported as is jitter (randomised intervals).

Default behaviour (that can be overridden) is to retry these HTTP status codes:

```
400, 403, 429, 500, 502, 503, 509
```
When the http client throws an exception it is retried if it is an `IOException` or an `UncheckedIOException`.

Default values for retries are:

| Parameter | Default           |
| ------------- |-------------:|
| Max Attempts | 4 |
| Initial Interval | 100ms      |
| Exponential Backoff Factor | 2      |
| Max Interval | 20s      |
| Jitter | 0 (none) |

The retry interval after attempt N is calculated like this:
```
interval = initialInterval * (backoffFactor ^ (N - 1)) * (1 - jitter * Math.random())
```

For example, using the defaults the retry intervals would be 100ms, 200ms, 400ms and then failure would be propagated. If you don't want exponential backoff then set that parameter to 1.

```java
Client s3 = Client
  .s3()
  .defaultClient()
  .retryMaxAttempts(10)
  .retryInitialInterval(100, TimeUnit.MILLISECONDS)
  .retryBackoffFactor(2.0)
  .retryMaxInterval(30, TimeUnit.SECONDS)
  .retryJitter(0.5)
  .retryStatusCodes(400, 403, 429, 500, 502, 503)
  .retryException(e -> false) // never retry exceptions
  .build();
```
Most of the same options are available on request builders:
```java
String content = s3
  .path("myBucket", "myObject.txt")
  .connectTimeout(5, TimeUnit.SECONDS)
  .readTimeout(5, TimeUnit.SECONDS)
  .retryMaxAttempts(3)
  .retryInitialInterval(5, TimeUnit.SECONDS)
  .responseAsUtf8();
```
You can also completely control the request retry (when there is an HTTP status code) via these builder methods:
```java
Client s3 = Client
  .s3()
  .defaultClient()
  .retryCondition(ris -> ris.statusCode() == 500)
  .retryException(e -> e instanceof IOException
                       || e instanceof UncheckedIOException)
...
```

### Presigned URLs
Presigned URLs are generated as follows (with a specified expiry duration):

```java
String presignedUrl =
  s3
    .path(bucketName, objectName)
    .presignedUrl(1, TimeUnit.DAYS));
```

### S3
The code below demonstrates the following:
* create bucket
* put object with metadata
* read object and metadata
* list objects in bucket
* delete object
* delete bucket

```java
// we'll create a random bucket name
String bucketName = "temp-bucket-" + System.currentTimeMillis();

///////////////////////
// create bucket
///////////////////////

String createXml = Xml
    .create("CreateBucketConfiguration")
    .a("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
    .e("LocationConstraint").content(region)
    .toString();
s3.path(bucketName)
    .method(HttpMethod.PUT)
    .requestBody(createXml)
    .execute();

////////////////////////////
// put object with metadata
///////////////////////////

String objectName = "ExampleObject.txt";
s3
    .path(bucketName, objectName)
    .method(HttpMethod.PUT)
    .requestBody("hi there")
    .metadata("category", "something")
    .execute();

///////////////////////////////////
// read object including metadata
///////////////////////////////////

String text = s3
    .path(bucketName + "/" + objectName)
    .responseAsUtf8();

///////////////////////////////////
// read object
///////////////////////////////////

Response r = s3
    .path(bucketName, objectName)
    .response();
System.out.println("response ok=" + response.isOk());
System.out.println(r.content().length + " chars read");
System.out.println("category=" + r.metadata("category").orElse(""));

///////////////////////////////////
// list bucket objects
///////////////////////////////////

List<String> keys = s3
    .url("https://" + bucketName + ".s3." + region + ".amazonaws.com")
    .query("list-type", "2")
    .responseAsXml()
    .childrenWithName("Contents")
    .stream()
    .map(x -> x.content("Key"))
    .collect(Collectors.toList());
System.out.println(keys);

///////////////////////////////////
// delete object
///////////////////////////////////

s3.path(bucketName, objectName)
    .method(HttpMethod.DELETE)
    .execute();

///////////////////////////////////
// delete bucket
///////////////////////////////////

s3.path(bucketName)
  .method(HttpMethod.DELETE)
  .execute();
```

### SQS
Here are some SQS tasks:

* create an sqs queue
* get the queue url
* place two messages on the queue
* read all messages of the queue and mark them as read
* delete the sqs queue

You'll note that most of the interactions with sqs involve using the url of the queue rather than the base service endpoint (`http://sqs.amazonaws.com`).

```java
String queueName = "MyQueue-" + System.currentTimeMillis();

///////////////////////////////////
// create queue
///////////////////////////////////

sqs.query("Action", "CreateQueue")
    .query("QueueName", queueName)
    .execute();

///////////////////////////////////
// get queue url
///////////////////////////////////

String queueUrl = sqs
    .query("Action", "GetQueueUrl")
    .query("QueueName", queueName)
    .responseAsXml()
    .content("GetQueueUrlResult", "QueueUrl");

///////////////////////////////////
// send a message
///////////////////////////////////

sqs.url(queueUrl)
    .query("Action", "SendMessage")
    .query("MessageBody", "hi there")
    .execute();

///////////////////////////////////
// read all messages
///////////////////////////////////

List<XmlElement> list;
do {
    list = sqs.url(queueUrl)
        .query("Action", "ReceiveMessage")
        .responseAsXml()
        .child("ReceiveMessageResult")
        .children();

    list.forEach(x -> {
      String msg = x.child("Body").content();
      System.out.println(msg);
      // mark message as read
      sqs.url(queueUrl)
              .query("Action", "DeleteMessage")
              .query("ReceiptHandle", x.child("ReceiptHandle").content())
              .execute();
    });
} while (!list.isEmpty());

///////////////////////////////////
// delete queue
///////////////////////////////////

sqs.url(queueUrl)
    .query("Action", "DeleteQueue")
    .execute();
```

### Recipes
See [Recipes](https://github.com/davidmoten/aws-lightweight-client-java/wiki/Recipes) for many more examples.

### Attributes
Some of the AWS API services (like SQS) represent property maps in the query string like this `?Attribute.Name.1=size&Attribute.Value.1=large&Attribute.Name.2=color&Attribute.Value.2=red`. The request builder has helper methods to do this for you:

```java
// Create a FIFO queue
String queueUrl = sqs.query("Action", "CreateQueue")
  .query("QueueName", queueName(applicationName, key))
  .attribute("FifoQueue", "true")
  .attribute("ContentBasedDeduplication", "true")
  .attribute("MessageRetentionPeriod", String.valueOf(TimeUnit.DAYS.toSeconds(14)))
  .attribute("VisibilityTimeout", "30")
  .responseAsXml()
  .content("CreateQueueResult", "QueueUrl");
```

When the prefix of the attribute is different, say "MessageProperty" instead of "Attribute" then you can use the `.attributePrefix(String)` method before calling `.attribute(String)`.

### Metadata
To set a header `x-amz-meta-KEY:VALUE` use the builder method `.metadata(KEY, VALUE)`.

### Error handling
Let's look at a simple one, reading an object in an S3 bucket.

```java
String text = s3
    .path(bucketName + "/" + objectName)
    .responseAsUtf8();
```
If the object does not exist an exception will be thrown like this:
```
com.github.davidmoten.aws.lw.client.ServiceException: statusCode=404: <?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>not-there</Key><RequestId>1TVAXX4VF5DYHJJH</RequestId><HostId>VrvGCPhExKbjuONSuX/LGw0mYSndjg3t26LNAQCKTL/i5U+cZfYa4ow3KQ1tpJdQuMH9sB4JTUk=</HostId></Error>
  at com.github.davidmoten.aws.lw.client.internal.ExceptionFactoryDefault.create(ExceptionFactoryDefault.java:17)
  at com.github.davidmoten.aws.lw.client.Request.responseAsBytes(Request.java:140)
  at com.github.davidmoten.aws.lw.client.Request.responseAsUtf8(Request.java:153)
  at com.github.davidmoten.aws.lw.client.ClientMain.main(ClientMain.java:48)
```

You can see that the AWS exception message (in xml format) is present in the error message and can be used to check for standard codes. If you were using the full AWS SDK library then it would throw a `NoSuchKeyException`. In our case we check for the presence of `NoSuchKey` in the error message.

The code below does not throw an exception when the object does not exist. However, `response.isOk()` returns false:

```java
Response r = s3
    .path(bucketName + "/" + objectName)
    .response();
System.out.println("ok=" + r.isOk() + ", statusCode=" + r.statusCode() + ", message=" + r.contentUtf8());
```

The output is:
```
ok=false, statusCode=404, message=<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>notThere</Key><RequestId>4AAX24QZ8777FA6B</RequestId><HostId>4N1rsMjjdM7tjKSQDXNQZNH8EOqNckUsO6gRVPfcjMmHZ9APRwYJwufZOr9l1Qlinux5W537bDc=</HostId></Error>
```
### Custom exceptions
You can define what exceptions get thrown using a builder method for a `Client`:

```java
Client sqs = Client
    .sqs()
    .defaultClient()
    .exception(
            x -> !x.isOk() && x.contentUtf8().contains("NonExistentQueue"),
            x -> new QueueDoesNotExistException(x.contentUtf8())
    .build()
```
You can add multiple exception handlers like above or you can set an `ExceptionFactory`. Any response not matching the criteria will
throw a `ServiceException` (in those circumstances where exceptions are thrown, like `.responseAsBytes()`, `.responseAsUtf8()` and `.responseAsXml()`).

## TODO
* Can a faster cold-start be had using Bouncy Castle TLS library?
* add debug logging?

