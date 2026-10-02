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

// Unified OAuth 2.0 request/response/fault handler for the Apigee oauth proxy.
(function() {
  function fail(statusCode, error, description) {
    context.setVariable('oauth.failed', 'true');
    context.setVariable('oauth.status_code', String(statusCode));
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

  function matchesCsvUri(redirectUri, csvValue) {
    if (!csvValue || String(csvValue).trim() === '') {
      return false;
    }
    var parts = String(csvValue).split(',');
    for (var i = 0; i < parts.length; i++) {
      if (parts[i].trim() === redirectUri) {
        return true;
      }
    }
    return false;
  }

  function isExactRedirectUriAllowed(redirectUri, allowedCsv, singleCallbackUri) {
    if (!redirectUri) {
      return false;
    }
    return matchesCsvUri(redirectUri, allowedCsv) || matchesCsvUri(redirectUri, singleCallbackUri);
  }

  function generateCsrfNonce() {
    var sha256 = crypto.getSHA256();
    sha256.update([
      context.getVariable('messageid') || '',
      context.getVariable('system.uuid') || '',
      context.getVariable('system.timestamp') || String(Date.now()),
      context.getVariable('request.queryparam.code_challenge') || '',
      context.getVariable('request.queryparam.state') || '',
      context.getVariable('private.oauth.uid_hmac_key') || ''
    ].join(':'));
    return sha256.digest64()
      .replace(/=+$/, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');
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

  var flowScope = context.flow || 'PROXY_REQ_FLOW';
  var flowName = context.getVariable('current.flow.name') || '';
  var host = context.getVariable('oauth.public_hostname') || '';

  // 1. FaultRule execution: map Apigee policy faults to OAuth 2.0 JSON error fields.
  // Guard against soft faults from continueOnError="true" lookup policies before validation completes.
  var faultName = context.getVariable('fault.name') || '';
  var clientFaults = {
    'InvalidApiKey': true,
    'FailedToResolveAPIKey': true,
    'InvalidApiKeyForGivenResource': true,
    'DeveloperStatusNotActive': true,
    'invalid_client': true,
    'InvalidClientIdentifier': true
  };
  var isClientFault = Boolean(clientFaults[faultName]) || (faultName === 'FailedToDecode' && flowName === 'token');
  var inFaultRule = isClientFault ||
    (flowName === 'callback' && context.getVariable('oauth.callback.step') === 'post_token') ||
    (flowName === 'token' && context.getVariable('oauth.token.validated') === 'true') ||
    (flowName !== 'callback' && flowName !== 'token');
  if (faultName && inFaultRule) {
    if (isClientFault) {
      fail(401, 'invalid_client', 'Invalid client_id.');
    } else {
      fail(400, 'invalid_grant', 'Invalid, expired, or already used authorization code or state.');
    }
    return;
  }

  // 2. Proxy Response Flow: construct discovery JSON or 302 redirects.
  if (flowScope === 'PROXY_RESP_FLOW') {
    if (flowName === 'as-metadata') {
      context.setVariable('response.status.code', 200);
      context.setVariable('response.header.Content-Type', 'application/json');
      context.setVariable('response.header.Cache-Control', 'public, max-age=300');
      context.setVariable('response.content', JSON.stringify({
        issuer: 'https://' + host + '/oauth',
        authorization_endpoint: 'https://' + host + '/oauth/authorize',
        token_endpoint: 'https://' + host + '/oauth/token',
        response_types_supported: ['code'],
        grant_types_supported: ['authorization_code', 'refresh_token'],
        token_endpoint_auth_methods_supported: ['client_secret_basic', 'client_secret_post'],
        code_challenge_methods_supported: ['S256'],
        scopes_supported: ['mcp']
      }));
      return;
    }

    if (flowName === 'protected-resource-metadata') {
      context.setVariable('response.status.code', 200);
      context.setVariable('response.header.Content-Type', 'application/json');
      context.setVariable('response.header.Cache-Control', 'public, max-age=300');
      context.setVariable('response.content', JSON.stringify({
        resource: 'https://' + host + '/oauth/mcp',
        authorization_servers: ['https://' + host + '/oauth'],
        scopes_supported: ['mcp'],
        bearer_methods_supported: ['header']
      }));
      return;
    }

    if (flowName === 'authorize') {
      var googleClientId = context.getVariable('private.oauth.google_client_id') || '';
      var state2Code = context.getVariable('oauthv2authcode.oauth-generate-state2-code.code') || '';
      var csrfNonce = context.getVariable('oauth.authorize.csrf_nonce') || '';
      var googleAuthUrl = 'https://accounts.google.com/o/oauth2/v2/auth'
        + '?client_id=' + encodeURIComponent(googleClientId)
        + '&redirect_uri=' + encodeURIComponent('https://' + host + '/oauth/callback')
        + '&response_type=code&scope=openid'
        + '&state=' + encodeURIComponent(state2Code);
      context.setVariable('response.status.code', 302);
      context.setVariable('response.header.Location', googleAuthUrl);
      context.setVariable('response.header.Set-Cookie', 'dc_oauth_state=' + csrfNonce + '; HttpOnly; Secure; SameSite=Lax; Path=/oauth/callback; Max-Age=600');
      context.setVariable('response.header.Cache-Control', 'no-store');
      return;
    }

    if (flowName === 'callback') {
      var appRedirectUri = context.getVariable('oauth.callback.redirect_uri') || '';
      var issuedAuthCode = context.getVariable('oauthv2authcode.oauth-generate-auth-code.code') || '';
      var encodedState1 = context.getVariable('oauth.callback.encoded_state1') || '';
      var sep = appRedirectUri.indexOf('?') === -1 ? '?' : '&';
      var appLocation = appRedirectUri + sep + 'code=' + encodeURIComponent(issuedAuthCode) + '&state=' + encodedState1;
      context.setVariable('response.status.code', 302);
      context.setVariable('response.header.Location', appLocation);
      context.setVariable('response.header.Set-Cookie', 'dc_oauth_state=; HttpOnly; Secure; SameSite=Lax; Path=/oauth/callback; Max-Age=0');
      context.setVariable('response.header.Cache-Control', 'no-store');
      return;
    }
    return;
  }

  // 3. Proxy Request Flow
  if (flowName === 'authorize') {
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
      fail(400, 'invalid_request', 'Missing or unauthorized redirect_uri.');
      return;
    }
    if (responseType !== 'code') {
      fail(400, 'unsupported_response_type', 'Only response_type=code is supported.');
      return;
    }
    if (scope === null || scope === undefined || String(scope).trim() === '') {
      scope = 'mcp';
    } else {
      scope = String(scope).trim();
    }
    if (scope !== 'mcp') {
      fail(400, 'invalid_scope', 'Only scope=mcp is supported.');
      return;
    }
    if (codeChallengeMethod !== 'S256') {
      fail(400, 'invalid_request', 'PKCE code_challenge_method=S256 is required.');
      return;
    }
    var pkcePattern = /^[A-Za-z0-9\-._~]{43,128}$/;
    if (!pkcePattern.test(codeChallenge)) {
      fail(400, 'invalid_request', 'Missing or invalid PKCE code_challenge.');
      return;
    }

    var nonce = generateCsrfNonce();
    context.setVariable('request.formparam.client_id', clientId);
    context.setVariable('request.queryparam.scope', scope);
    context.setVariable('oauth.authorize.client_id', clientId);
    context.setVariable('oauth.authorize.redirect_uri', redirectUri);
    context.setVariable('oauth.authorize.response_type', 'code');
    context.setVariable('oauth.authorize.scope', scope);
    context.setVariable('oauth.authorize.state1', state1);
    context.setVariable('oauth.authorize.code_challenge', codeChallenge);
    context.setVariable('oauth.authorize.code_challenge_method', 'S256');
    context.setVariable('oauth.authorize.csrf_nonce', nonce);
    context.setVariable('oauth.authorize.issued_at_ms', String(Date.now()));
    return;
  }

  if (flowName === 'callback') {
    var callbackStep = context.getVariable('oauth.callback.step') || '';
    if (callbackStep === 'post_token') {
      var rawTokenResp = context.getVariable('googleTokenResponse.content') || '';
      var idToken = '';
      try {
        var parsed = JSON.parse(rawTokenResp);
        idToken = parsed && parsed.id_token ? String(parsed.id_token) : '';
      } catch (e) {
        idToken = '';
      }
      if (!idToken) {
        fail(400, 'invalid_grant', 'Failed to exchange Google authorization code for id_token.');
        return;
      }
      context.setVariable('private.oauth.google_id_token', idToken);
      // Set default error metadata in case sub claim check fails after VerifyJWT.
      context.setVariable('oauth.status_code', '400');
      context.setVariable('oauth.error', 'invalid_grant');
      context.setVariable('oauth.error_description', 'Invalid Google ID token.');
      return;
    }

    var upstreamError = context.getVariable('request.queryparam.error') || '';
    var googleCode = context.getVariable('request.queryparam.code') || '';
    var state2 = context.getVariable('request.queryparam.state') || '';
    if (upstreamError || !googleCode || !state2) {
      fail(400, 'invalid_grant', 'Missing or rejected authorization parameters.');
      return;
    }

    var authStep = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.auth_step') || '';
    var expectedNonce = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.csrf_nonce') || '';
    var issuedAtMs = Number(context.getVariable('oauthv2authcode.oauth-get-state2-attributes.issued_at_ms') || '0');
    var nowMs = Date.now();
    if (authStep !== 'authorize' || !expectedNonce || !issuedAtMs || (nowMs - issuedAtMs) > 600000) {
      fail(400, 'invalid_grant', 'Invalid, expired, or already used OAuth state.');
      return;
    }

    var cookieHeader = context.getVariable('request.header.cookie') || context.getVariable('request.header.Cookie') || '';
    var cookieNonce = getCookieValue(cookieHeader, 'dc_oauth_state') || '';
    if (!constantTimeEquals(cookieNonce, expectedNonce)) {
      fail(400, 'invalid_request', 'OAuth state cookie mismatch.');
      return;
    }

    var cbClientId = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.client_id') || '';
    var cbRedirectUri = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.redirect_uri') || '';
    var cbResponseType = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.response_type') || 'code';
    var cbScope = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.scope') || 'mcp';
    var cbState1 = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.state1') || '';
    var cbChallenge = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.code_challenge') || '';
    var cbChallengeMethod = context.getVariable('oauthv2authcode.oauth-get-state2-attributes.code_challenge_method') || 'S256';

    context.setVariable('oauth.google_code', googleCode);
    context.setVariable('oauth.state2', state2);
    context.setVariable('request.formparam.client_id', cbClientId);
    context.setVariable('request.queryparam.client_id', cbClientId);
    context.setVariable('request.queryparam.redirect_uri', cbRedirectUri);
    context.setVariable('request.queryparam.response_type', cbResponseType);
    context.setVariable('request.queryparam.scope', cbScope);
    context.setVariable('oauth.callback.redirect_uri', cbRedirectUri);
    context.setVariable('oauth.callback.state1', cbState1);
    context.setVariable('oauth.callback.encoded_state1', encodeURIComponent(cbState1));
    context.setVariable('oauth.callback.code_challenge', cbChallenge);
    context.setVariable('oauth.callback.code_challenge_method', cbChallengeMethod);
    context.setVariable('oauth.callback.step', 'post_token');
    return;
  }

  if (flowName === 'token') {
    if (typeof context.removeVariable === 'function') {
      context.removeVariable('request.header.Authorization');
    }
    var expectedSecret = context.getVariable('verifyapikey.oauth-verify-client-form.client_secret') || '';
    var providedSecret = context.getVariable('request.formparam.client_secret') || '';
    if (!constantTimeEquals(providedSecret, expectedSecret)) {
      fail(401, 'invalid_client', 'Invalid client credentials.');
      return;
    }

    var grantType = context.getVariable('request.formparam.grant_type') || '';
    if (grantType !== 'authorization_code' && grantType !== 'refresh_token') {
      fail(400, 'unsupported_grant_type', 'Only authorization_code and refresh_token grant types are supported.');
      return;
    }
    if (grantType === 'refresh_token') {
      var refreshToken = context.getVariable('request.formparam.refresh_token') || '';
      if (!refreshToken) {
        fail(400, 'invalid_grant', 'Missing refresh_token parameter.');
        return;
      }
      context.setVariable('oauth.token.validated', 'true');
      return;
    }

    var codeAuthStep = context.getVariable('oauthv2authcode.oauth-get-code-attributes.auth_step') || '';
    var uid = context.getVariable('oauthv2authcode.oauth-get-code-attributes.uid') || '';
    if (codeAuthStep !== 'callback' || !uid) {
      fail(400, 'invalid_grant', 'Invalid authorization code.');
      return;
    }

    var expectedRedirectUri = context.getVariable('oauthv2authcode.oauth-get-code-attributes.redirect_uri') || '';
    var providedRedirectUri = context.getVariable('request.formparam.redirect_uri') || '';
    if (!expectedRedirectUri || providedRedirectUri !== expectedRedirectUri) {
      fail(400, 'invalid_grant', 'Mismatched redirect_uri.');
      return;
    }

    var storedChallenge = context.getVariable('oauthv2authcode.oauth-get-code-attributes.code_challenge') || '';
    var storedMethod = context.getVariable('oauthv2authcode.oauth-get-code-attributes.code_challenge_method') || '';
    var codeVerifier = context.getVariable('request.formparam.code_verifier') || '';
    var verifierPattern = /^[A-Za-z0-9\-._~]{43,128}$/;
    if (storedMethod !== 'S256' || !storedChallenge || !verifierPattern.test(codeVerifier)) {
      fail(400, 'invalid_grant', 'Missing or invalid PKCE code_verifier.');
      return;
    }

    var sha256 = crypto.getSHA256();
    sha256.update(codeVerifier);
    var computedChallenge = sha256.digest64()
      .replace(/=+$/, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');

    if (!constantTimeEquals(computedChallenge, storedChallenge)) {
      fail(400, 'invalid_grant', 'PKCE verification failed.');
      return;
    }
    context.setVariable('oauth.token.validated', 'true');
    return;
  }

  if (flowName === 'not-found') {
    fail(404, 'not_found', 'The requested OAuth endpoint or resource does not exist.');
  }
})();
