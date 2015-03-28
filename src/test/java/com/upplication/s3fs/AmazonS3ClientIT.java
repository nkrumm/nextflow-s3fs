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

package com.upplication.s3fs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.upplication.s3fs.util.EnvironmentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static com.upplication.s3fs.util.EnvironmentBuilder.getRealEnv;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertNotNull;

public class AmazonS3ClientIT {
	
	AmazonS3Client client;
	
	@Before
	public void setup() throws IOException{
		// s3client
		final Map<String, Object> credentials = getRealEnv();
		BasicAWSCredentials credentialsS3 = new BasicAWSCredentials(credentials.get(S3FileSystemProvider.ACCESS_KEY).toString(), 
				credentials.get(S3FileSystemProvider.SECRET_KEY).toString());
		AmazonS3 s3 = new com.amazonaws.services.s3.AmazonS3Client(credentialsS3);
		client = new AmazonS3Client(s3);
	}
	
	@Test
	public void putObject() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), randomUUID().toString(), file.toFile());
	
		assertNotNull(result);
	}
	
	@Test
	public void putObjectWithEndSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), randomUUID().toString() + "/", file.toFile());
	
		assertNotNull(result);
	}
	
	@Test(expected = AmazonS3Exception.class)
	public void putObjectWithStartSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		client.putObject(getBucket(), "/" + randomUUID().toString(), file.toFile());
	}
	
	@Test(expected = AmazonS3Exception.class)
	public void putObjectWithBothSlash() throws IOException{
		Path file = Files.createTempFile("file-se", "file");
		Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);
		
		PutObjectResult result = client.putObject(getBucket(), "/" + randomUUID().toString() + "/", file.toFile());
	
		assertNotNull(result);
	}
	
	@Test
	public void putObjectByteArray() throws IOException{
		
		PutObjectResult result = client
				.putObject(getBucket(), randomUUID().toString(), new ByteArrayInputStream("contenido1".getBytes()),
						new ObjectMetadata());
	
		assertNotNull(result);
	}
	
	private String getBucket() {
		return EnvironmentBuilder.getBucket().replace("/", "");
	}
}