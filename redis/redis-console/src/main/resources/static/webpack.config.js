const path = require('path');

module.exports = {
  entry: './main.ts',
  mode: 'production',
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
      {
        test: /\.coffee$/,
        use: 'coffee-loader'
      },
      { 
        test: /ng-table\/.*\.html$/, 
        use: ['ngtemplate-loader?requireAngular&relativeTo=/src/browser/&prefix=ng-table/', 'html-loader'] 
      },
      {
        test: /\.(jpg|png|gif)$/,
        loader: 'url-loader'
      },
    ],
  },
  resolve: {
    alias: {
      "@angular/upgrade/static": "@angular/upgrade/bundles/upgrade-static.umd.js"
    },
    extensions: ['.tsx', '.ts', '.js'],
  },
  output: {
    filename: 'bundle.js',
    path: path.resolve(__dirname, 'dist'),
  }
};
