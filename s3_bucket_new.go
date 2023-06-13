package main

import (
    // "os"
    "fmt"
    "context"
    // "io"
    // "log"
    "errors"
    
    // "github.om/aws/aws-sdk-go/aws"
    // "github.om/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    // "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
    
    // "github.om/aws/aws-sdk-go/aws/session"

    // "github.om/aws/aws-sdk-go/service/s3/s3manager"

)

// func DownloadFile(downloader *s3manager.Downloader, bucketName string, key string) error {
//     file, err := os.Create(key)
//     if err != nil {
//         return err
//     }
    
//     defer file.Close()
    
//     _, err = downloader.Download(
//         file, 
//         &s3.GetObjectInput{
//             Bucket: aws.String(bucketName),
//             Key:    aws.String(key),
        
//         },
//     )
//     return err
// }


// func DownloadFile(s3Client, bucketName string, objectKey string, fileName string) error {
// 	result, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
// 		Bucket: aws.String(bucketName),
// 		Key:    aws.String(objectKey),
// 	})
// 	if err != nil {
// 		log.Printf("Couldn't get object %v:%v. Here's why: %v\n", bucketName, objectKey, err)
// 		return err
// 	}
// 	defer result.Body.Close()
// 	file, err := os.Create(fileName)
// 	if err != nil {
// 		log.Printf("Couldn't create file %v. Here's why: %v\n", fileName, err)
// 		return err
// 	}
// 	defer file.Close()
// 	body, err := io.ReadAll(result.Body)
// 	if err != nil {
// 		log.Printf("Couldn't read object body from %v. Here's why: %v\n", objectKey, err)
// 	}
// 	_, err = file.Write(body)
// 	return err
// }



func DownloadS3File(objectKey string, bucket string, s3Client *s3.Client) ([]byte, error) {

    buffer := manager.NewWriteAtBuffer([]byte{})

    downloader := manager.NewDownloader(s3Client)

    numBytes, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(objectKey),
    })
    if err != nil {
        return nil, err
    }

    if numBytes < 1 {
        return nil, errors.New("zero bytes written to memory")
    }

    return buffer.Bytes(), nil
}



// func main() {
//     sess, err := session.NewSessionWithOptions(session.Options{
//         Profile: "default",
//         Config: aws.Config{
//             Region: aws.String("us-west-2"),
//         },
//     })

//     if err != nil {
//         fmt.Printf("Failed to initialize new session: %v", err)
//         return
//     }
    
//     bucketName := ""
//     downloader := s3manager.NewDownloader(sess)
//     key := "1.jpg"
//     err = DownloadFile(downloader, bucketName, key)
    
//     if err != nill {
//         fmt.Printf("Couldn't download file: %v", err)
//         return
//     }
                   
//     fmt.Println("Successfully downloaded file")

// }

func main() {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	count := 10
	fmt.Printf("Let's list up to %v buckets for your account.\n", count)
	result, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		fmt.Printf("Couldn't list buckets for your account. Here's why: %v\n", err)
		return
	}
	if len(result.Buckets) == 0 {
		fmt.Println("You don't have any buckets!")
	} else {
		if count > len(result.Buckets) {
			count = len(result.Buckets)
		}
		for _, bucket := range result.Buckets[:count] {
			fmt.Printf("\t%v\n", *bucket.Name)
		}
	}
    
    bucketName := "open5gs-respons-logs"
    objectKey := "prometheus-metrics/01GZG6J9GB7C4ASTP3AQE83RP3/"
    // fileName := "01GZG6J9GB7C4ASTP3AQE83RP3"
    result, err = DownloadS3File(bucketName, objectKey, s3Client)
    
    if err != nil {
        fmt.Printf("Couldn't download file: %v", err)
        return
    }
                   
    fmt.Println("Successfully downloaded file")
    
}
