/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
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

package com.upplication.s3fs;

import com.upplication.s3fs.util.AmazonS3ClientMock;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class AmazonS3ClientMockBuilder {

    private FileSystem fs;
    private Path bucket;

    public AmazonS3ClientMockBuilder(FileSystem fs){
        this.fs = fs;
    }

    /**
     * create the base bucket
     * @param bucket
     * @return
     */
    public AmazonS3ClientMockBuilder withBucket(String bucket){
        try {
            this.bucket = Files.createDirectories(fs.getPath("/" + bucket));
        }
        catch (IOException e) {
           throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * add directories to the bucket
     * @param dir String path with optional '/' separator for subdirectories
     * @return
     */
    public AmazonS3ClientMockBuilder withDirectory(String dir){
        try {
            Files.createDirectories(this.bucket.resolve(dir));
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * add a file with content to the bucket
     * @param file String path with optional '/' separator for directories
     * @param content
     * @return
     */
    public AmazonS3ClientMockBuilder withFile(String file, String content){
        try {
            Path filepath = prepareFile(file);

            Files.write(filepath, content.getBytes());
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * add a empty file to the bucket
     * @param file
     * @return
     */
    public AmazonS3ClientMockBuilder withFile(String file){
        try {
            Path filepath = prepareFile(file);
            Files.createFile(filepath);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    /**
     * build and associate the AmazonS3Client to the S3Provider
     * @param provider S3FileSystemProvider
     * @return AmazonS3ClientMock and ready to stub with mockito.
     */
    public AmazonS3ClientMock build(S3FileSystemProvider provider){
        try {
            AmazonS3ClientMock clientMock = spy(new AmazonS3ClientMock(fs.getPath("/")));
            S3FileSystem s3ileS3FileSystem = new S3FileSystem(provider, clientMock, "endpoint");
            doReturn(s3ileS3FileSystem).when(provider).createFileSystem(any(URI.class), anyObject(), anyObject());
            return clientMock;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Path prepareFile(String file) throws IOException {
        Path filepath = this.bucket.resolve(file);
        if (filepath.getParent() != null){
            Files.createDirectories(filepath.getParent());
        }
        return filepath;
    }

}
