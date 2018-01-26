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

package com.upplication.s3fs.util;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.FilesOperationsIT;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static com.upplication.s3fs.S3FileSystemProvider.ACCESS_KEY;
import static com.upplication.s3fs.S3FileSystemProvider.SECRET_KEY;
/**
 * Test Helper
 */
public abstract class EnvironmentBuilder {
	
	public static final String BUCKET_NAME_KEY = "bucket_name";
	/**
	 * Get credentials from environment vars, and if not found from amazon-test.properties
	 * @return Map with the credentials
	 */
	public static Map<String, Object> getRealEnv(){
		Map<String, Object> env = null;
		
		String accessKey = System.getenv(ACCESS_KEY);
		String secretKey = System.getenv(SECRET_KEY);
		
		if (accessKey != null && secretKey != null){
			env = ImmutableMap.<String, Object> builder()
				.put(ACCESS_KEY, accessKey)
				.put(SECRET_KEY, secretKey).build();
		}
		else{
			final Properties props = new Properties();
			try {
				props.load(EnvironmentBuilder.class.getResourceAsStream("/amazon-test.properties"));
			} catch (IOException e) {
				throw new RuntimeException("not found amazon-test.properties in the classpath", e);
			}
			env = ImmutableMap.<String, Object> builder()
					.put(ACCESS_KEY, props.getProperty(ACCESS_KEY))
					.put(SECRET_KEY, props.getProperty(SECRET_KEY)).build();
		}
		
		return env;
	}
	/**
	 * get default bucket name
	 * @return String without end separator
	 */
	public static String getBucket(){
		
		String bucketName = System.getenv(BUCKET_NAME_KEY);
		if (bucketName != null){
			return bucketName;
		}
		else{
			final Properties props = new Properties();
			try {
				props.load(FilesOperationsIT.class.getResourceAsStream("/amazon-test.properties"));
				return props.getProperty(BUCKET_NAME_KEY);
			} catch (IOException e) {
				throw new RuntimeException("needed /amazon-test.properties in the classpath");
			}
		}
	}
}
