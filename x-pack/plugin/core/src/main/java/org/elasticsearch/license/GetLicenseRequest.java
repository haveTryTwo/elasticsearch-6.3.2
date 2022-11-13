/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;


public class GetLicenseRequest extends MasterNodeReadRequest<GetLicenseRequest> {

    public GetLicenseRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
