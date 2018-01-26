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


import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class S3IteratorTest {
    public static final URI S3_GLOBAL_URI = URI.create("s3:///");
    S3FileSystemProvider provider;
    FileSystem fsMem;

    @Before
    public void cleanup() throws IOException {
        fsMem = MemoryFileSystemBuilder.newLinux().build("linux");
        try{
            FileSystems.getFileSystem(S3_GLOBAL_URI).close();
        }
        catch(FileSystemNotFoundException e){}

        provider = spy(new S3FileSystemProvider());
        // TODO: we need some real temp dir with unique path when is called
        doReturn(Files.createDirectory(fsMem.getPath("/" + UUID.randomUUID().toString())))
                .doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .when(provider).createTempDir();
        doReturn(new Properties()).when(provider).loadAmazonProperties();
    }

    @After
    public void closeMemory() throws IOException{
        fsMem.close();
    }

    @Test
    public void iteratorDirectory() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir/file1")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir/");

        assertIterator(iterator, "file1");
    }



    @Test
    public void iteratorAnotherDirectory() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir2/file1")
                .withFile("dir2/file2")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir2/");

        assertIterator(iterator, "file1", "file2");
    }

    @Test
    public void iteratorWithFileContainsDirectoryName() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir2/dir2-file")
                .withFile("dir2-file2")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir2/");

        assertIterator(iterator, "dir2-file");
    }

    @Test
    public void iteratorWithSubFolderAndSubFiles() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir/file")
                .withFile("dir/file2")
                .withFile("dir/dir/file")
                .withFile("dir/dir2/file")
                .withFile("dir/dir2/dir3/file3")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir/");

        assertIterator(iterator, "file", "file2", "dir", "dir2");
    }

    @Test
    public void iteratorWithSubFolderAndSubFilesAtBucketLevel() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("file")
                .withFile("file2")
                .withFile("dir/file")
                .withFile("dir2/file")
                .withFile("dir2/dir3/file3")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "/");

        assertIterator(iterator, "file", "file2", "dir", "dir2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void iteratorKeyNotEndSlash() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir2/dir2-file")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir2");
    }

    @Test
    public void iteratorFileReturnEmpty() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("file1")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "file1/");

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorEmptyDirectory() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withDirectory("dir")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir/");

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorBucket() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("file1")
                .withFile("file2")
                .withFile("file3")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "/");

        assertIterator(iterator, "file1", "file2", "file3");
    }

    @Test
    public void iteratorMoreThanAmazonS3ClientLimit() throws IOException {
        AmazonS3ClientMockBuilder builder =new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA");

        String filesNameExpected[] = new String[1050];
        for (int i = 0; i < 1050; i++){
            final String name = "file-" + i;
            builder.withFile(name);
            filesNameExpected[i] = name;
        }

        builder.build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "/");

        assertIterator(iterator, filesNameExpected);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir/file1")
                .build(provider);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator((S3FileSystem)fileSystem, "bucketA", "dir/");
        iterator.remove();
    }


    private Map<String, ?> buildFakeEnv(){
        return ImmutableMap.<String, Object> builder()
                .put(S3FileSystemProvider.ACCESS_KEY, "access key")
                .put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
    }

    private void assertIterator(Iterator<Path> iterator, final String ... files) throws IOException {

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        Set<String> filesNamesExpected = new HashSet<>(Arrays.asList(files));
        Set<String> filesNamesActual = new HashSet<>();

        while (iterator.hasNext()) {
            Path path = iterator.next();
            String fileName = path.getFileName().toString();
            filesNamesActual.add(fileName);
        }

        assertEquals(filesNamesExpected, filesNamesActual);
    }
}
