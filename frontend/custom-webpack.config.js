/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const { LicenseWebpackPlugin } = require("license-webpack-plugin");

// Workaround for license-webpack-plugin v4 crashing with `ENOENT: scandir ''` when a
// bundled module has no resolvable on-disk directory. Some @codingame/monaco-vscode-*
// sub-modules surface as virtual entries (descriptionFileRoot === '') after the v25
// upgrade. Make listPaths/readFileAsUtf8/isDirectory tolerant of empty paths.
{
  const fs = require("fs");
  const wfs = require("license-webpack-plugin/dist/WebpackFileSystem").WebpackFileSystem;
  const origListPaths = wfs.prototype.listPaths;
  wfs.prototype.listPaths = function (path) {
    if (!path) return [];
    try {
      return origListPaths.call(this, path);
    } catch (e) {
      if (e && e.code === "ENOENT") return [];
      throw e;
    }
  };
  const origIsDirectory = wfs.prototype.isDirectory;
  wfs.prototype.isDirectory = function (path) {
    if (!path) return false;
    try {
      return origIsDirectory.call(this, path);
    } catch (e) {
      if (e && e.code === "ENOENT") return false;
      throw e;
    }
  };
  const origReadFileAsUtf8 = wfs.prototype.readFileAsUtf8;
  wfs.prototype.readFileAsUtf8 = function (path) {
    if (!path) return "";
    try {
      return origReadFileAsUtf8.call(this, path);
    } catch (e) {
      if (e && e.code === "ENOENT") return "";
      throw e;
    }
  };
}

// Match CSS files shipped by codingame's monaco-vscode-* family (and the editor-api
// alias of monaco-editor / vscode that points at them). These ship as Constructable
// Stylesheet imports — they must skip style-loader and use css-loader's
// `exportType: 'css-style-sheet'` mode. See the codingame troubleshooting wiki:
// https://github.com/CodinGame/monaco-vscode-api/wiki/Troubleshooting
const codingameCssRe = /node_modules[\\/](?:@codingame[\\/]monaco-vscode-[^\\/]+|monaco-editor|vscode)[\\/].*\.css$/;

module.exports = {
  module: {
    rules: [
      {
        // The codingame monaco-vscode-* family ships raw assets (.svg/.ttf/.png/.woff*)
        // that webpack must emit as static files rather than parse as JS.
        test: /\.(svg|ttf|woff2?|png|jpg|jpeg|gif)$/,
        include: [require("path").resolve(__dirname, "node_modules/@codingame")],
        type: "asset/resource",
      },
      {
        test: /\.css$/,
        oneOf: [
          {
            test: codingameCssRe,
            use: [
              {
                loader: "css-loader",
                options: {
                  esModule: false,
                  exportType: "css-style-sheet",
                  url: true,
                  import: true,
                },
              },
            ],
          },
          {
            use: ["style-loader", "css-loader"],
            include: [
              require("path").resolve(__dirname, "node_modules/monaco-breakpoints"),
            ],
          },
        ],
      },
    ],
    // Enable URL handling in webpack's JavaScript parser, required for loading .wasm files.
    // See https://github.com/angular/angular-cli/issues/24617
    parser: {
      javascript: {
        url: true,
      },
    },
  },
  resolve: {
    // css-loader emits relative imports (e.g. '../../../../../../../css-loader/dist/runtime/api.js')
    // computed from the source CSS location. The codingame monaco-vscode-* packages live one
    // namespace level deeper (`node_modules/@codingame/...`) than css-loader assumes, so the
    // emitted path lands at `node_modules/@codingame/css-loader/...` instead of
    // `node_modules/css-loader/...`. Alias the missing leg back to the real install.
    alias: {
      [require("path").resolve(__dirname, "node_modules/@codingame/css-loader")]:
        require("path").resolve(__dirname, "node_modules/css-loader"),
      [require("path").resolve(__dirname, "node_modules/@codingame/style-loader")]:
        require("path").resolve(__dirname, "node_modules/style-loader"),
    },
  },
  plugins: [
    new LicenseWebpackPlugin({
      perChunkOutput: false,
      outputFilename: "3rdpartylicenses.json",
      // Skip packages whose resolved directory is missing or unreadable. The codingame
      // monaco-vscode-* family ships sub-modules whose package roots license-webpack-plugin
      // can't always locate, and a missing license file shouldn't fail the build.
      handleMissingLicenseText: () => null,
      excludedPackageTest: (name) => !name,
      renderLicenses: (modules) =>
        JSON.stringify(
          modules
            .map((m) => ({
              name: m.packageJson && m.packageJson.name,
              version: m.packageJson && m.packageJson.version,
              license: m.licenseId,
            }))
            .filter((e) => e.name && e.version)
            .sort((a, b) =>
              a.name === b.name
                ? a.version.localeCompare(b.version)
                : a.name.localeCompare(b.name),
            ),
          null,
          2,
        ),
    }),
  ],
};
