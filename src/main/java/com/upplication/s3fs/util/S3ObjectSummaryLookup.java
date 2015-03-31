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

/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Javier Arnáiz @arnaix
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.upplication.s3fs.util;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.upplication.s3fs.AmazonS3Client;
import com.upplication.s3fs.S3Path;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

public class S3ObjectSummaryLookup {

    /**
     * Get the {@link com.amazonaws.services.s3.model.S3ObjectSummary} that represent this Path or her first child if this path not exists
     * @param s3Path {@link com.upplication.s3fs.S3Path}
     * @return {@link com.amazonaws.services.s3.model.S3ObjectSummary}
     * @throws java.nio.file.NoSuchFileException if not found the path and any child
     */
    public S3ObjectSummary lookup(S3Path s3Path) throws NoSuchFileException {

        S3ObjectSummary summary = s3Path.fetchObjectSummary();
        if( summary != null ) {
            return summary;
        }

        AmazonS3Client client = s3Path.getFileSystem().getClient();

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(s3Path.getBucket());
        request.setPrefix(s3Path.getKey());
        request.setMaxKeys(1);
        ObjectListing current = client.listObjects(request);

        if (!current.getObjectSummaries().isEmpty()){
            return current.getObjectSummaries().get(0);
        }
        else {
            throw new NoSuchFileException("s3://" + s3Path.getBucket() + "/" + s3Path.toString());
        }
    }

    @Deprecated
    public ObjectMetadata getS3ObjectMetadata(S3Path s3Path) {
        AmazonS3Client client = s3Path.getFileSystem().getClient();
        try {
            return client.getObjectMetadata(s3Path.getBucket(), s3Path.getKey());
        }
        catch (AmazonS3Exception e){
            if (e.getStatusCode() != 404){
                throw e;
            }
            return null;
        }
    }

    /**
     * get S3Object represented by this S3Path try to access with or without end slash '/'
     * @param s3Path S3Path
     * @return S3Object or null if not exists
     */
    @Deprecated
    private S3Object getS3Object(S3Path s3Path){

        AmazonS3Client client = s3Path.getFileSystem()
                .getClient();

        S3Object object = getS3Object(s3Path.getBucket(), s3Path.getKey(), client);

        if (object != null) {
            return object;
        }
        else{
            return getS3Object(s3Path.getBucket(), s3Path.getKey() + "/", client);
        }
    }

    /**
     * get s3Object with S3Object#getObjectContent closed
     * @param bucket String bucket
     * @param key String key
     * @param client AmazonS3Client client
     * @return S3Object
     */
    @Deprecated
    private S3Object getS3Object(String bucket, String key, AmazonS3Client client){
        try {
            S3Object object = client
                    .getObject(bucket, key);
            // FIXME: how only get the metadata
            if (object.getObjectContent() != null){
                object.getObjectContent().close();
            }
            return object;
        }
        catch (AmazonS3Exception e){
            if (e.getStatusCode() != 404){
                throw e;
            }
            return null;
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
