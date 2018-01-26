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
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class PathSpecTest {

	FileSystem fs;

	@Before
	public void setup() throws IOException {
		fs = MemoryFileSystemBuilder.newLinux().build("linux");
	}

	@After
	public void close() throws IOException {
		fs.close();
	}

    // first and more

    @Test
    public void firstAndMore(){
        assertEquals(fs.getPath("/dir","dir", "file"), fs.getPath("/dir", "dir/file"));
        assertEquals(fs.getPath("/dir/dir/file"), fs.getPath("/dir", "dir/file"));
    }

	// absolute relative

	@Test
	public void relative() {
		assertTrue(!get("file").isAbsolute());
	}

	@Test
	public void absolute() {
		assertTrue(get("/file/file2").isAbsolute());
	}

	// test starts with

	@Test
	public void startsWith() {
		assertTrue(get("/file/file1").startsWith(get("/file")));
	}

	@Test
	public void startsWithBlank() {
		assertFalse(get("/file").startsWith(get("")));
	}
	
	@Test
   	public void startsWithBlankRelative(){
		assertFalse(get("file1").startsWith(get("")));
	}
      

	@Test
	public void startsWithBlankBlank() {
		assertTrue(get("").startsWith(get("")));
	}

	@Test
	public void startsWithRelativeVsAbsolute() {
		assertFalse(get("/file/file1").startsWith(get("file")));
	}

	@Test
	public void startsWithFalse() {
		assertFalse(get("/file/file1").startsWith(get("/file/file1/file2")));
		assertTrue(get("/file/file1/file2").startsWith(get("/file/file1")));
	}

	@Test
	public void startsWithNotNormalize() {
		assertFalse(get("/file/file1/file2").startsWith(get("/file/file1/../")));
	}

	@Test
	public void startsWithNormalize() {
		assertTrue(get("/file/file1/file2").startsWith(
				get("/file/file1/../").normalize()));
	}

	@Test
	public void startsWithRelative() {
		assertTrue(get("file/file1").startsWith(get("file")));
	}

	@Test
	public void startsWithString() {
		assertTrue(get("/file/file1").startsWith("/file"));
	}

	// ends with

	@Test
	public void endsWithAbsoluteRelative() {
		assertTrue(get("/file/file1").endsWith(get("file1")));
	}

	@Test
	public void endsWithAbsoluteAbsolute() {
		assertTrue(get("/file/file1").endsWith(get("/file/file1")));
	}

	@Test
	public void endsWithRelativeRelative() {
		assertTrue(get("file/file1").endsWith(get("file1")));
	}

	@Test
	public void endsWithRelativeAbsolute() {
		assertFalse(get("file/file1").endsWith(get("/file")));
	}

	@Test
	public void endsWithDifferenteFileSystem() {
		assertFalse(get("/file/file1").endsWith(Paths.get("/file/file1")));
	}

	@Test
	public void endsWithBlankRelativeAbsolute() {
		assertFalse(get("").endsWith(get("/bucket")));
	}
	
	@Test
	public void endsWithBlankBlank() {
		assertTrue(get("").endsWith(get("")));
	}

	@Test
	public void endsWithRelativeBlankAbsolute() {
		assertFalse(get("/bucket/file1").endsWith(get("")));
	}
	
	@Test
 	public void endsWithRelativeBlankRelative(){
 		assertFalse(get("file1").endsWith(get("")));
 	}

    // file name

    @Test
    public void getFileName() throws IOException {
        try (FileSystem windows = MemoryFileSystemBuilder.newWindows().build("widows")){
            Path fileName = windows.getPath("C:/file").getFileName();
            Path rootName = windows.getPath("C:/").getFileName();

            assertEquals(windows.getPath("file"), fileName);
            assertNull(rootName);
        }
    }

	// ~ helpers methods

	private Path get(String path) {
		return fs.getPath(path);
	}
}
