/*
 * Copyright (c) 2013-2015, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2015, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.upplication.s3fs;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.StorageClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Model a S3 multipart upload request
 */
class S3UploadRequest extends S3MultipartOptions<S3UploadRequest> {

    private static final Logger log = LoggerFactory.getLogger(S3UploadRequest.class);

    /**
     * ID of the S3 object to store data into.
     */
    private S3ObjectId objectId;

    /**
     * Amazon S3 storage class to apply to the newly created S3 object, if any.
     */
    private StorageClass storageClass;

    /**
     * Metadata that will be attached to the stored S3 object.
     */
    private ObjectMetadata metadata;



    S3UploadRequest() {

    }

    S3UploadRequest(Properties props) {
        super(props);
        setStorageClass(props.getProperty("upload_storage_class"));
        setStorageEncryption(props.getProperty("storage_encryption"));
    }

    public S3ObjectId getObjectId() {
        return objectId;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ObjectMetadata getMetadata() {
        return metadata;
    }


    public S3UploadRequest setObjectId(S3ObjectId objectId) {
        this.objectId = objectId;
        return this;
    }

    public S3UploadRequest setStorageClass(StorageClass storageClass) {
        this.storageClass = storageClass;
        return this;
    }

    public S3UploadRequest setStorageClass(String storageClass) {
        if( storageClass==null ) return this;

        try {
            setStorageClass( StorageClass.fromValue(storageClass) );
        }
        catch( IllegalArgumentException e ) {
            log.warn("Not a valid AWS S3 storage class: `{}` -- Using default", storageClass);
        }
        return this;
    }


    public S3UploadRequest setStorageEncryption(String storageEncryption) {
        if( storageEncryption == null) {
            return this;
        }
        else if (!"AES256".equals(storageEncryption)) {
            log.warn("Not a valid S3 server-side encryption type: `{}` -- Currently only AES256 is supported",storageEncryption);
        }
        else {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            this.setMetadata(objectMetadata);
        }
        return this;
    }

    public S3UploadRequest setMetadata(ObjectMetadata metadata) {
        this.metadata = metadata;
        return this;
    }


}