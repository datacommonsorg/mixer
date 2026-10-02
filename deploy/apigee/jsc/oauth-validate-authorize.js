// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Validates GET /oauth/authorize query parameters after VerifyAPIKey succeeds
// and prepares inputs for generating the state2 authorization code.
(function() {
  function fail(error, description) {
    context.setVariable('oauth.error', error);
    context.setVariable('oauth.error_description', description);
  }

  function isExactRedirectUriAllowed(redirectUri, allowedCsv, singleCallbackUri) {
    if (!redirectUri) {
      return false;
    }
    if (allowedCsv && String(allowedCsv).trim() !== '') {
      var parts = String(allowedCsv).split(',');
      for (var i = 0; i < parts.length; i++) {
        if (parts[i].trim() === redirectUri) {
          return true;
        }
      }
      return false;
    }
    if (singleCallbackUri && String(singleCallbackUri).trim() !== '') {
      return String(singleCallbackUri).trim() === redirectUri;
    }
    return false;
  }

  function generateCsrfNonce() {
    var chars = '0123456789abcdef';
    var out = '';
    for (var i = 0; i < 32; i++) {
      out += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return out;
  }

  var clientId = context.getVariable('request.queryparam.client_id') || '';
  var redirectUri = context.getVariable('request.queryparam.redirect_uri') || '';
  var responseType = context.getVariable('request.queryparam.response_type') || '';
  var scope = context.getVariable('request.queryparam.scope');
  var state1 = context.getVariable('request.queryparam.state') || '';
  var codeChallenge = context.getVariable('request.queryparam.code_challenge') || '';
  var codeChallengeMethod = context.getVariable('request.queryparam.code_challenge_method') || '';

  var allowedCsv = context.getVariable('verifyapikey.oauth-verify-client-id.allowed_redirect_uris');
  var singleCallbackUri = context.getVariable('verifyapikey.oauth-verify-client-id.redirection_uris');

  if (!isExactRedirectUriAllowed(redirectUri, allowedCsv, singleCallbackUri)) {
    fail('invalid_request', 'Missing or unauthorized redirect_uri.');
    return;
  }

  if (responseType !== 'code') {
    fail('unsupported_response_type', 'Only response_type=code is supported.');
    return;
  }

  if (scope === null || scope === undefined || String(scope).trim() === '') {
    scope = 'mcp';
  } else {
    scope = String(scope).trim();
  }
  if (scope !== 'mcp') {
    fail('invalid_scope', 'Only scope=mcp is supported.');
    return;
  }

  if (codeChallengeMethod !== 'S256') {
    fail('invalid_request', 'PKCE code_challenge_method=S256 is required.');
    return;
  }

  var pkcePattern = /^[A-Za-z0-9\-._~]{43,128}$/;
  if (!pkcePattern.test(codeChallenge)) {
    fail('invalid_request', 'Missing or invalid PKCE code_challenge.');
    return;
  }

  var csrfNonce = generateCsrfNonce();

  context.setVariable('request.formparam.client_id', clientId);
  context.setVariable('request.queryparam.scope', scope);
  context.setVariable('oauth.authorize.client_id', clientId);
  context.setVariable('oauth.authorize.redirect_uri', redirectUri);
  context.setVariable('oauth.authorize.response_type', 'code');
  context.setVariable('oauth.authorize.scope', scope);
  context.setVariable('oauth.authorize.state1', state1);
  context.setVariable('oauth.authorize.code_challenge', codeChallenge);
  context.setVariable('oauth.authorize.code_challenge_method', 'S256');
  context.setVariable('oauth.authorize.csrf_nonce', csrfNonce);
  context.setVariable('oauth.authorize.issued_at_ms', String(Date.now()));
})();
