Package.describe({
  name: 'iyyang:cfs-aliyun',
  version: '0.0.1',
  summary: 'Aliyun OSS storage adaptger for CollectionFS',
  git: 'https://github.com/yyang/cfs-aliyun.git'
});

Npm.depends({
  'aliyun-sdk': '1.6.3',
});

Package.onUse(function(api) {
  api.versionsFrom('1.0');

  api.use(['cfs:base-package@0.0.30', 'cfs:storage-adapter@0.2.1']);
  api.addFiles([
    'aliyun.server.js',
    'aliyun.stream.js',
    ], 'server');
  api.addFiles('aliyun.client.js', 'client');
});

// Package.onTest(function(api) {
//   api.use(['cfs:standard-packages', 'iyyang:cfs-aliyun', 'test-helpers', 'tinytest'], 'server');
//   api.addFiles('tests/server-tests.js', 'server');
//   api.addFiles('tests/client-tests.js', 'client');
// });
