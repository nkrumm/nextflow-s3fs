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

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created by IntelliJ IDEA. User: bbejeck Date: 1/23/12 Time: 10:29 PM
 */
public class CopyDirVisitor extends SimpleFileVisitor<Path> {

	private Path fromPath;
	private Path toPath;
	private StandardCopyOption copyOption;

	public CopyDirVisitor(Path fromPath, Path toPath,
			StandardCopyOption copyOption) {
		this.fromPath = fromPath;
		this.toPath = toPath;
		this.copyOption = copyOption;
	}

	public CopyDirVisitor(Path fromPath, Path toPath) {
		this(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public FileVisitResult preVisitDirectory(Path dir,
			BasicFileAttributes attrs) throws IOException {

		// permitimos resolver entre distintos providers
		Path targetPath = appendPath(dir);

		if (!Files.exists(targetPath)) {
			if (!targetPath.getFileName().toString().endsWith("/")){
				targetPath = targetPath.getParent().resolve(targetPath.getFileName().toString() + "/");
			}
			Files.createDirectory(targetPath);
		}
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
			throws IOException {

		Path targetPath = appendPath(file);

		Files.copy(file, targetPath, copyOption);
		return FileVisitResult.CONTINUE;
	}

	/**
	 * Obtenemos el path que corresponde en el parametro: {@link #fromPath}
	 * relativo al parametro <code>Path to</code>
	 * 
	 * @param to
	 *            Path
	 * @return
	 */
	private Path appendPath(Path to) {
		Path targetPath = toPath;
		// sacamos el path relativo y lo recorremos para
		// añadirlo al nuevo

		for (Path path : fromPath.relativize(to)) {
			// si utilizamos path en vez de string: lanza error por ser
			// distintos paths
			targetPath = targetPath.resolve(path.getFileName().toString());
		}

		return targetPath;
	}
}