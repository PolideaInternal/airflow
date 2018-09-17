# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow import AirflowException, LoggingMixin
import re

COMPOSITE_FIELD_TYPES = ['union', 'dict']


class FieldValidationException(AirflowException):
    """
    Thrown when validation finds dictionary field not valid according to specification.
    """

    def __init__(self, message):
        super(FieldValidationException, self).__init__(message)


class ValidationSpecificationException(AirflowException):
    """
    Thrown when validation specification is wrong
    (rather than dictionary being validated).
    This should only happen during development as ideally
     specification itself should not be invalid ;) .
    """

    def __init__(self, message):
        super(ValidationSpecificationException, self).__init__(message)


class FieldValidator(LoggingMixin):
    """
    Validates correctness of dictionary according to specification.
    The specification can describe various type of
    fields including custom validation, and union of fields. This validator is meant
    to be reusable by various operators
    in the near future, but for now it is left as part of the Google Cloud Function,
    so documentation about the
    validator is not yet complete. To see what kind of specification can be used,
    please take a look at
    gcf_function_deploy_operator.py which specifies validation for GCF deploy.

    TODO: make better description, add some examples and move it to
          contrib/utils folder when we reuse it.
    """
    def __init__(self, validation_specs, api):
        # type: ([dict], str) -> None
        super(FieldValidator, self).__init__()
        self._validation_specs = validation_specs
        self._api = api

    @staticmethod
    def _get_field_name_with_parent(field_name, parent):
        if parent:
            return parent + '.' + field_name
        return field_name

    @staticmethod
    def _sanity_checks(children_validation_specs, field_type, full_field_path,
                       regexp, custom_validation, value):
        # type: (dict, str, str, str, function, object) -> None
        if value is None and field_type != 'union':
            raise FieldValidationException(
                "The required field '{}' is missing".format(full_field_path))
        if regexp and field_type:
            raise ValidationSpecificationException(
                "The field '{}' has both type and regexp defined. The regexp is only"
                "allowed without type (i.e. assume type is 'str' "
                "that can be validated with regexp)".format(full_field_path))
        if children_validation_specs and field_type not in COMPOSITE_FIELD_TYPES:
            raise ValidationSpecificationException(
                "Fields are specified in field '{}' which is of type '{}'. "
                "Fields are only allowed for composite fields ('{}').".
                format(full_field_path, field_type, COMPOSITE_FIELD_TYPES))
        if custom_validation and field_type:
            raise ValidationSpecificationException(
                "The field '{}' has both type and custom_validation defined. "
                "Custom validation is only allowed without type so that "
                "custom validation can check type on its own".
                format(full_field_path))

    @staticmethod
    def _validate_regexp(full_field_path, regexp, value):
        # type: (str, str, str) -> None
        if not re.match(regexp, value):
            # Note matching of only the beginning as we assume the regexps all-or-nothing
            raise FieldValidationException(
                "The field '{}' value '{}' does not match regexp {}.".
                format(full_field_path, value, regexp))

    def _validate_dict(self, children_validation_specs, full_field_path, value):
        # type: (dict, str, dict) -> None
        for child_validation_spec in children_validation_specs:
            self._validate_field(validation_spec=child_validation_spec,
                                 dictionary_to_validate=value,
                                 parent=full_field_path)
        # Possibly we find out that we should not validate extra fields
        # for future compatibility ?
        # But this is super helpful to detect typos early,
        # so we might want to leave it here. It's super easy
        # To add future validations
        # (and we might want to disable this check with some flag)
        for field_name in value.keys():
            if field_name not in [spec['name'] for spec in children_validation_specs]:
                self.log.warning(
                    "The field '{}' is present in the dictionary but "
                    "is missing in validation specification '{}'".format(
                        self._get_field_name_with_parent(field_name, full_field_path),
                        children_validation_specs))

    def _validate_union(self, children_validation_specs, full_field_path,
                        dictionary_to_validate):
        # type: (dict, str, dict) -> None
        field_found = False
        found_field_name = None
        for child_validation_spec in children_validation_specs:
            # Forcing optional so that we do not have to type optional = True
            # in specification for all union fields
            new_field_found = self._validate_field(
                validation_spec=child_validation_spec,
                dictionary_to_validate=dictionary_to_validate,
                parent=full_field_path,
                force_optional=True)
            field_name = child_validation_spec['name']
            if new_field_found and field_found:
                raise FieldValidationException(
                    "The field '{}' and '{}' are part of '{}' union so they are mutually "
                    "exclusive. They should not be both present in the same dictionary!".
                    format(field_name, found_field_name, full_field_path))
            if new_field_found:
                field_found = True
                found_field_name = field_name
        if not field_found:
            self.log.warning(
                "None of available variants of '{}' union was found in dictionary {}!. "
                "Should be one of {}".format(
                    full_field_path,
                    dictionary_to_validate,
                    [field['name'] for field in children_validation_specs]))

    def _validate_field(self, validation_spec, dictionary_to_validate, parent=None,
                        force_optional=False):
        """
        Validates if field is OK.
        :param validation_spec: specification of the field
        :param dictionary_to_validate: dictionary where the field should be present
        :param parent: full path of parent field
        :param force_optional: forces the field to be optional
        (all union fields have force_optional set to True)
        :return: True if the field is present
        """
        field_name = validation_spec['name']
        field_type = validation_spec.get('type')
        optional = validation_spec.get('optional')
        regexp = validation_spec.get('regexp')
        children_validation_specs = validation_spec.get('fields')
        required_api = validation_spec.get('api')
        custom_validation = validation_spec.get('custom_validation')

        full_field_path = self._get_field_name_with_parent(field_name=field_name,
                                                           parent=parent)
        if required_api and required_api != self._api:
            self.log.debug(
                "Not validating the field '{}' for api version '{}' "
                "as it is only required for api '{}'".
                format(field_name, self._api, required_api))
            return False
        value = dictionary_to_validate.get(field_name)

        if (optional or force_optional) and value is None:
            self.log.debug("The optional field '{}' is missing, but that's perfectly ok".
                           format(full_field_path))
            return False

        # Certainly down from here the field is present (value is not None)
        # so we should only return True from now on

        self._sanity_checks(children_validation_specs=children_validation_specs,
                            field_type=field_type,
                            full_field_path=full_field_path,
                            regexp=regexp,
                            custom_validation=custom_validation,
                            value=value)

        if regexp:
            self._validate_regexp(full_field_path, regexp, value)
        elif field_type == 'dict':
            if not isinstance(value, dict):
                raise FieldValidationException(
                    "The field '{}' should be a dictionary and is '{}'"
                    .format(full_field_path, value))
            if children_validation_specs is None:
                self.log.debug(
                    "The dict field '{}' has no children fields defined, "
                    "but that's perfectly ok. "
                    "We just skip validating dict elements.".format(full_field_path))
            else:
                self._validate_dict(children_validation_specs, full_field_path, value)
        elif field_type == 'union':
            if not children_validation_specs:
                raise ValidationSpecificationException(
                    "The union field '{}' has no children "
                    "defined which is clearly wrong!".format(full_field_path))
            self._validate_union(children_validation_specs, full_field_path,
                                 dictionary_to_validate)
        elif custom_validation:
            try:
                custom_validation(value)
            except Exception as e:
                raise FieldValidationException(
                    "Error while validating custom field '{}': {}".
                    format(full_field_path, e))
        else:
            raise ValidationSpecificationException(
                "The field '{}' is of type '{}' which I have no idea about!".format(
                    full_field_path, type))

        return True

    def validate(self, dictionary_to_validate):
        """
        Validates if the dictionary follows specification that the validator was
        instantiated with. Raises ValidationSpecificationException or
        ValidationFieldException in case of problems with specification or the
        dictionary not conforming to the specification respectively.
        :param dictionary_to_validate: dictionary that must follow the specification
        :return: None
        """
        try:
            for validation_spec in self._validation_specs:
                self._validate_field(validation_spec=validation_spec,
                                     dictionary_to_validate=dictionary_to_validate)
        except FieldValidationException as e:
            raise FieldValidationException(
                "There was an error when validating: '{}': {}".
                format(dictionary_to_validate, e))
