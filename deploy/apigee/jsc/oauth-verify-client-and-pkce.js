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

// Verifies client_secret and (for POST /oauth/token authorization_code grants)
// checks auth_step, redirect_uri, and PKCE S256 code_verifier.
(function() {
  function fail(error, description) {
    context.setVariable('oauth.error', error);
    context.setVariable('oauth.error_description', description);
  }

  function constantTimeEquals(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string' || a.length === 0 || a.length !== b.length) {
      return false;
    }
    var diff = 0;
    for (var i = 0; i < a.length; i++) {
      diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
    }
    return diff === 0;
  }

  var expectedSecret = context.getVariable('verifyapikey.oauth-verify-client-form.client_secret') || '';
  var providedSecret = context.getVariable('request.formparam.client_secret') || '';
  if (!constantTimeEquals(providedSecret, expectedSecret)) {
    context.setVariable('oauth.client_invalid', 'true');
    return;
  }

  var pathSuffix = context.getVariable('proxy.pathsuffix') || '';
  if (pathSuffix === '/revoke') {
    var tokenToRevoke = context.getVariable('request.formparam.token') || '';
    if (!tokenToRevoke) {
      fail('invalid_request', 'Missing token parameter.');
    }
    return;
  }

  var grantType = context.getVariable('request.formparam.grant_type') || '';
  if (grantType !== 'authorization_code' && grantType !== 'refresh_token') {
    fail('unsupported_grant_type', 'Only authorization_code and refresh_token grant types are supported.');
    return;
  }

  if (grantType === 'refresh_token') {
    var refreshToken = context.getVariable('request.formparam.refresh_token') || '';
    if (!refreshToken) {
      fail('invalid_grant', 'Missing refresh_token parameter.');
    }
    return;
  }

  // grant_type === 'authorization_code'
  var authStep = context.getVariable('oauthv2authcode.oauth-get-code-attributes.auth_step') || '';
  var uid = context.getVariable('oauthv2authcode.oauth-get-code-attributes.uid') || '';
  if (authStep !== 'callback' || !uid) {
    fail('invalid_grant', 'Invalid authorization code.');
    return;
  }

  var expectedRedirectUri = context.getVariable('oauthv2authcode.oauth-get-code-attributes.redirect_uri') || '';
  var providedRedirectUri = context.getVariable('request.formparam.redirect_uri') || '';
  if (!expectedRedirectUri || providedRedirectUri !== expectedRedirectUri) {
    fail('invalid_grant', 'Mismatched redirect_uri.');
    return;
  }

  var codeChallenge = context.getVariable('oauthv2authcode.oauth-get-code-attributes.code_challenge') || '';
  var codeChallengeMethod = context.getVariable('oauthv2authcode.oauth-get-code-attributes.code_challenge_method') || '';
  var codeVerifier = context.getVariable('request.formparam.code_verifier') || '';
  var verifierPattern = /^[A-Za-z0-9\-._~]{43,128}$/;
  if (codeChallengeMethod !== 'S256' || !codeChallenge || !verifierPattern.test(codeVerifier)) {
    fail('invalid_grant', 'Missing or invalid PKCE code_verifier.');
    return;
  }

  var sha256 = crypto.getSHA256();
  sha256.update(codeVerifier);
  var computedChallenge = sha256.digest64()
    .replace(/=+$/, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');

  if (!constantTimeEquals(computedChallenge, codeChallenge)) {
    fail('invalid_grant', 'PKCE verification failed.');
    return;
  }
})();
