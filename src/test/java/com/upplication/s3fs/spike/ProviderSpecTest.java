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

package com.upplication.s3fs.spike;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class ProviderSpecTest {
	FileSystem fs;

	@Before
	public void setup() throws IOException {
		fs = MemoryFileSystemBuilder.newLinux().build("linux");
	}

	@After
	public void close() throws IOException {
		fs.close();
	}

    @Test
    public void readNothing() throws IOException {
        //Path base = Files.createDirectories(fs.getPath("/dir"));
        Path base = Files.createTempDirectory("asdadadasd");

        try (SeekableByteChannel seekable = Files.newByteChannel(Files.createFile(base.resolve("file1.html")),
                EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE))){
        }

        assertTrue(Files.notExists(base.resolve("file1.html")));

    }

	// FIXME @Test
	public void seekable() throws IOException{
		Path base = Files.createDirectories(fs.getPath("/dir"));
		// in windows throw exception
		try (SeekableByteChannel seekable = Files.newByteChannel(base.resolve("file1.html"), 
				EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, 
				StandardOpenOption.READ))){
			
			ByteBuffer buffer =  ByteBuffer.wrap("content".getBytes());
			seekable.position(7);
			seekable.write(buffer);

			ByteBuffer bufferRead = ByteBuffer.allocate(7);
			bufferRead.clear();
			seekable.read(bufferRead);
			
			assertArrayEquals(bufferRead.array(), buffer.array());
		}
	}
	
	@Test
	public void seekableRead() throws IOException{
		/*
		Path base = Files.createDirectories(fs.getPath("/dir"));
		Path path = Files.write(base.resolve("file"), "contenido yuhu".getBytes(), StandardOpenOption.CREATE_NEW);
		*/
		Path path = Files.write(Files.createTempFile("asdas", "asdsadad"), "contenido uyuhu".getBytes(), StandardOpenOption.APPEND);
		try (SeekableByteChannel channel = Files.newByteChannel(path)) {

			//channel = Paths.get("Path to file").newByteChannel(StandardOpenOption.READ);
		    ByteBuffer buffer = ByteBuffer.allocate(4096);

		    System.out.println("File size: " + channel.size());

		    while (channel.read(buffer) > 0) {
		        buffer.rewind();
		        
		        System.out.print(new String(buffer.array(), 0, buffer.remaining()));

		        buffer.flip();

		        System.out.println("Current position : " + channel.position());
		    }
			
			
			
			
			/*
			
			
			
			
			ByteBuffer buffer = ByteBuffer.allocate(1024);

		      sbc.position(4);
		      sbc.read(buffer);
		      for (int i = 0; i < 5; i++) {
		        System.out.print((char) buffer.get(i));
		      }

		      buffer.clear();
		      sbc.position(0);
		      sbc.read(buffer);
		      for (int i = 0; i < 4; i++) {
		        System.out.print((char) buffer.get(i));
		      }
		      sbc.position(0);
		      buffer = ByteBuffer.allocate(1024);
		      String encoding = System.getProperty("file.encoding");
		      int numberOfBytesRead = sbc.read(buffer);
		      System.out.println("Number of bytes read: " + numberOfBytesRead);
		      while (numberOfBytesRead > 0) {
		        buffer.rewind();
		        System.out.print("[" + Charset.forName(encoding).decode(buffer) + "]");
		        buffer.flip();
		        numberOfBytesRead = sbc.read(buffer);
		        System.out.println("\nNumber of bytes read: " + numberOfBytesRead);
		      }
*/
		    }
	}
}
