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

// Validates GET /oauth/callback state2 attributes and login-CSRF cookie, and
// prepares inputs for exchanging the Google code and issuing our auth code.
(function() {
  function fail(error, description) {
    context.setVariable('oauth.error', error);
    context.setVariable('oauth.error_description', description);
  }

  function getCookieValue(cookieHeader, name) {
    if (!cookieHeader) {
      return null;
    }
    var pairs = String(cookieHeader).split(';');
    for (var i = 0; i < pairs.length; i++) {
      var pair = pairs[i].trim();
      var eqIdx = pair.indexOf('=');
      if (eqIdx > 0) {
        var k = pair.substring(0, eqIdx).trim();
        var v = pair.substring(eqIdx + 1).trim();
        if (k === name) {
          return v;
        }
      }
    }
    return null;
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

  var googleCode = context.getVariable('request.queryparam.code') || '';
  var state2 = context.getVariable('request.queryparam.state') || '';
  if (!googleCode || !state2) {
    fail('invalid_request', 'Missing code or state parameter.');
    return;
  }

  var authStep = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.auth_step') || '';
  var expectedNonce = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.csrf_nonce') || '';
  var issuedAtMs = Number(context.getVariable('oauthv2authcode.oauth-get-state2-attributes.issued_at_ms') || '0');
  var nowMs = Date.now();
  if (authStep !== 'authorize' || !expectedNonce || !issuedAtMs || (nowMs - issuedAtMs) > 600000) {
    fail('invalid_grant', 'Invalid, expired, or already used OAuth state.');
    return;
  }

  var cookieHeader = context.getVariable('request.header.cookie') || context.getVariable('request.header.Cookie') || '';
  var cookieNonce = getCookieValue(cookieHeader, 'dc_oauth_state') || '';
  if (!constantTimeEquals(cookieNonce, expectedNonce)) {
    fail('invalid_request', 'OAuth state cookie mismatch.');
    return;
  }

  var clientId = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.client_id') || '';
  var redirectUri = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.redirect_uri') || '';
  var responseType = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.response_type') || 'code';
  var scope = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.scope') || 'mcp';
  var state1 = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.state1') || '';
  var codeChallenge = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.code_challenge') || '';
  var codeChallengeMethod = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.code_challenge_method') || 'S256';

  context.setVariable('oauth.google_code', googleCode);
  context.setVariable('oauth.state2', state2);

  context.setVariable('request.formparam.client_id', clientId);
  context.setVariable('request.queryparam.client_id', clientId);
  context.setVariable('request.queryparam.redirect_uri', redirectUri);
  context.setVariable('request.queryparam.response_type', responseType);
  context.setVariable('request.queryparam.scope', scope);

  context.setVariable('oauth.callback.redirect_uri', redirectUri);
  context.setVariable('oauth.callback.state1', state1);
  context.setVariable('oauth.callback.encoded_state1', encodeURIComponent(state1));
  context.setVariable('oauth.callback.code_challenge', codeChallenge);
  context.setVariable('oauth.callback.code_challenge_method', codeChallengeMethod);
})();
