import {
  BlobDeleteOptions,
  BlobDeleteResponse,
  BlobExistsOptions,
  BlobServiceClient,
  BlobUploadCommonResponse,
  BlockBlobParallelUploadOptions,
  StoragePipelineOptions,
} from "@azure/storage-blob";
import path from "path";
import mime from "mime-types";
import AWS from "aws-sdk";
import { PromiseResult } from "aws-sdk/lib/request";

let blobServiceClient: BlobServiceClient | null = null;
let s3: AWS.S3 | null = null;

export function connectAzureBlobStorage(
  connectionString: string,
  options: StoragePipelineOptions = {}
) {
  if (!blobServiceClient) {
    // Initialize BlobServiceClient
    blobServiceClient = BlobServiceClient.fromConnectionString(
      connectionString,
      options
    );
  }
  return blobServiceClient;
}

export function connectS3(options: AWS.S3.ClientConfiguration) {
  if (!s3) {
    s3 = new AWS.S3(options);
  }
  return s3;
}

export interface UniversalConnectionOptions {
  storagePipelineOptions?: StoragePipelineOptions;
  clientConfiguration?: AWS.S3.ClientConfiguration;
}

export function connectStorage(
  connectionString: string,
  options: UniversalConnectionOptions
) {
  const results: {
    blobServiceClient: BlobServiceClient | null;
    s3: AWS.S3 | null;
  } = { blobServiceClient: null, s3: null };
  if (
    connectionString.includes("accessKeyId") &&
    connectionString.includes("secretAccessKey") &&
    connectionString.includes("region")
  ) {
    const params: any = {
      ...(options.clientConfiguration || {}),
    };
    for (const kv of connectionString.trim().split(";")) {
      const [k, v] = kv.trim().split("=");
      params[k] = v;
    }
    results.s3 = connectS3(params);
  } else {
    results.blobServiceClient = connectAzureBlobStorage(
      connectionString,
      options.storagePipelineOptions
    );
  }
  return results;
}

export function getBlobServiceClient() {
  if (!blobServiceClient) {
    throw new Error("Azure Blob Storage is not initialized!");
  }
  return blobServiceClient;
}

export function getS3() {
  if (!s3) {
    throw new Error("S3 is not initialized!");
  }
  return s3;
}

export interface UploadBlobOptions {
  blobName: string;
  containerName: string;
  buffer: Buffer | Blob | ArrayBuffer | ArrayBufferView;
  uploadOptions?: BlockBlobParallelUploadOptions;
}

export async function uploadBlob(options: UploadBlobOptions) {
  const { blobName, containerName, buffer } = options;
  // Get the file extension
  const fileExtension = path.extname(blobName).slice(1);

  // Lookup the MIME type for the file extension
  const contentType = mime.lookup(fileExtension);
  const containerClient =
    getBlobServiceClient().getContainerClient(containerName);

  const uploadOptions = options.uploadOptions || {};
  // Define options for blob upload
  const defaultUploadOptions: BlockBlobParallelUploadOptions = {
    blobHTTPHeaders: {
      blobContentType: contentType || undefined,
    },
    ...uploadOptions,
  };

  // Upload to Azure Blob Storage
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  const uploadBlobResponse = await blockBlobClient.uploadData(
    buffer,
    defaultUploadOptions
  );
  return uploadBlobResponse;
}

export async function uploadToS3(
  params: AWS.S3.PutObjectRequest,
  options?: AWS.S3.ManagedUpload.ManagedUploadOptions
) {
  return await getS3()
    .upload(params, options || undefined)
    .promise();
}

export interface UniversalUploadOptions {
  azureOrS3: string;
  buffer: Buffer;
  fileName: string;
  containerOrBucketName: string;
}

export async function uploadToStorage(options: UniversalUploadOptions) {
  const results: {
    azure: BlobUploadCommonResponse | null;
    s3: AWS.S3.ManagedUpload.SendData | null;
  } = {
    azure: null,
    s3: null,
  };
  const { azureOrS3, containerOrBucketName, fileName, buffer } = options;
  if (azureOrS3 == "azure") {
    results.azure = await uploadBlob({
      containerName: containerOrBucketName,
      blobName: fileName,
      buffer,
    });
  } else {
    results.s3 = await uploadToS3({
      Bucket: containerOrBucketName,
      Key: fileName,
      Body: buffer,
    });
  }
  return results;
}

export interface DeleteBlobOptions {
  containerName: string;
  blobName: string;
  blobDeleteOptions?: BlobDeleteOptions;
}

export async function deleteBlob(options: DeleteBlobOptions) {
  const { containerName, blobName, blobDeleteOptions } = options;
  const containerClient =
    getBlobServiceClient().getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  return await blockBlobClient.delete(blobDeleteOptions || undefined);
}

export async function deleteFromS3(params: AWS.S3.DeleteObjectRequest) {
  return await getS3().deleteObject(params).promise();
}

export interface UniversalDeleteOptions {
  azureOrS3: string;
  fileName: string;
  containerOrBucketName: string;
}

export async function deleteFromStorage(options: UniversalDeleteOptions) {
  const { azureOrS3, fileName, containerOrBucketName } = options;
  const results: {
    blobDeleteResponse: BlobDeleteResponse | null;
    deleteObjectOutput: PromiseResult<
      AWS.S3.DeleteObjectOutput,
      AWS.AWSError
    > | null;
  } = {
    blobDeleteResponse: null,
    deleteObjectOutput: null,
  };
  if (azureOrS3 == "azure") {
    results.blobDeleteResponse = await deleteBlob({
      containerName: containerOrBucketName,
      blobName: fileName,
    });
  } else {
    results.deleteObjectOutput = await deleteFromS3({
      Bucket: containerOrBucketName,
      Key: fileName,
    });
  }
  return results;
}

export interface IsBlobExistsOptions {
  containerName: string;
  blobName: string;
  blobExistsOptions?: BlobExistsOptions;
}

export async function isBlobExists(options: IsBlobExistsOptions) {
  const { containerName, blobName, blobExistsOptions } = options;
  const containerClient =
    getBlobServiceClient().getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  return await blockBlobClient.exists(blobExistsOptions || undefined);
}

export async function isObjectExists(params: AWS.S3.HeadObjectRequest) {
  try {
    await getS3().headObject(params).promise();
    return true; // The file exists
  } catch (err: any) {
    if (err.code === "NotFound") {
      // The file does not exist
      return false;
    }
    // Some other error occurred
    throw err;
  }
}

export interface UniversalBlobOrObjectExistsOptions {
  azureOrS3: string;
  fileName: string;
  containerOrBucketName: string;
}

export async function isBlobOrObjectExists(
  options: UniversalBlobOrObjectExistsOptions
) {
  const { azureOrS3, fileName, containerOrBucketName } = options;
  if (azureOrS3 == "azure") {
    return await isBlobExists({
      containerName: containerOrBucketName,
      blobName: fileName,
    });
  } else {
    return await isObjectExists({
      Bucket: containerOrBucketName,
      Key: fileName,
    });
  }
}
