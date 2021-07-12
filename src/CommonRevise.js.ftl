<script language="javascript">

    <#assign hasModPrivilege = springMacroRequestContext.getWebApplicationContext().getBean("imasAuthManagerHelper").currentUserHasModBtnPrivilege(RequestParameters["targetGroupId"]!'', RequestParameters["reportCode"]!'')/>
    var hasModPrivilege = true;
    <#if !hasModPrivilege>
    hasModPrivilege = false;
    </#if>

    <#assign recordOpr = statics["com.gingkoo.rdms.base.data.maintenance.enums.RecordOpr"]>
    <#assign batchMode = statics["com.gingkoo.rdms.base.data.maintenance.enums.BatchMode"]>
    <#assign opFlag = statics["com.gingkoo.rdms.base.webapp.enums.OpFlagEnum"]>
    <#assign nextActionEnum = statics["com.gingkoo.rdms.base.webapp.enums.NextActionEnum"]>
    <#assign productMode = statics["com.gingkoo.gf4j2.framework.service.SysParamService"].getSysParamDef("IMAS","PRODUCT_OPR_MODE","BIZ")!''>

    var dataRptDate = "${RequestParameters["dataRptDate"]!''}";
    var targetGroupId = "${RequestParameters["targetGroupId"]!''}";
    var targetOrgId = "${RequestParameters["targetOrgId"]!''}";
    var checkFlag = "${RequestParameters["checkFlag"]!''}";
    var checkDesc = "${RequestParameters["checkDesc"]!''}";
    var pageType = "${RequestParameters["pageType"]!''}";
    var taskId = "${RequestParameters["taskId"]!''}";
    var taskCheckStatus = "${RequestParameters["checkStatus"]!''}";
    var pageStatus = "${RequestParameters["pageStatus"]!''}";
    var changeViewMode = "${RequestParameters["changeViewMode"]!'dataRptFlag'}";

    var Page_path = "/pages/imas/rpt/pagetpl/DetailPageTpl.ftl?reportCode=" + report_code;

    var datasetQueryCallback = null;

    var opr = "";
    var isForce = "N";
    var isCommit = "Y";
    var batchMode = "${batchMode.SINGLE}";

    if (pageType === "B") {
        var groupId = getFieldInitValue("select_groupId");
        Page_ds_dataset.setParameter("groupId", groupId);
        Page_ds_interface_dataset.setFieldRequired("groupId", true);
    }

    $(function () {
        if (pageType != "C") {
            try {
                $("#btReasonDown").hide();
                $("#btReasonUp").hide();
            } catch (e) {
            }
            try {
                var resultTable = $("#resultTb");
                resultTable.datagrid('hideColumn', "errReason");
                resultTable.datagrid('hideColumn', "errDesc");
            } catch (e) {
            }
        }
    })

    $(function () {
        var resultTable = $("#resultTb");
        if (pageType === "F" || (pageType === "R") || (pageType === "H")) {
            hideAllButton();
        }
        setInterfaceFieldHidden("nextAction");
        if (pageType === "F") {
            setInterfaceFieldHidden("dataFlag");
            setInterfaceFieldHidden("checkFlag");
            resultTable.datagrid('hideColumn', "select");
            resultTable.datagrid('hideColumn', "dataFlag");
            resultTable.datagrid('hideColumn', "nextAction");
            resultTable.datagrid('hideColumn', "checkFlag");
        }
        // 数据归档查询
        if (pageType === "H") {
            //隐藏查询条件
            setInterfaceFieldHidden("dataOp");
            setInterfaceFieldHidden("checkFlag");
            setInterfaceFieldHidden("dataReviseStatus");
            /* 显示拉链日期
            resultTable.datagrid('showColumn', "startDate");
            resultTable.datagrid('showColumn', "endDate");*/

            // 隐藏数据表列
            resultTable.datagrid('hideColumn', "select");
            resultTable.datagrid('hideColumn', "dataOp");
            resultTable.datagrid('hideColumn', "nextAction");
            resultTable.datagrid('hideColumn', "checkFlag");

            $("#BTN_CHECK_RESULT").hide();
            initSearch({fieldId: 'frmCXXX', tables: $('#frmCXXX').find(".grouptable"), maxW: 40, minW: 40})
        }
        if (pageStatus === "00") {
            $("#BTN_ROLLBACK").hide();
            $("#BTN_APPROVE").hide();
            $("#BTN_REJECT").hide();
            $("#BTN_SEND_BACK").hide();
        } else if (pageStatus === "10" || pageStatus === "20") {
            setInterfaceFieldHidden("dataReviseStatus");
            $("#BTN_ADD").hide();
            $("#BTN_DEL").hide();
            $("#BTN_CHECK").hide();
            $("#btUpload").hide();
            $("#btProblemExport").hide();
            $("#BTN_COMMIT").hide();
            $("#BTN_SEND_BACK").hide();
        } else if (pageStatus === "30") {
            setInterfaceFieldHidden("dataOp");
            resultTable.datagrid('hideColumn', "dataOp");
            $("#BTN_ADD").hide();
            $("#BTN_DEL").hide();
            $("#BTN_CHECK").hide();
            $("#btUpload").hide();
            $("#btProblemExport").hide();
            $("#BTN_COMMIT").hide();
            $("#BTN_ROLLBACK").hide();
            $("#BTN_APPROVE").hide();
            $("#BTN_REJECT").hide();
        } else {
            hideAllButton();
        }
        <#if meta.extra.oprMode == "BIZ">
        $("#BTN_COMMIT").hide();
        $("#BTN_ROLLBACK").hide();
        $("#BTN_APPROVE").hide();
        $("#BTN_REJECT").hide();
        $("#BTN_SEND_BACK").hide();
        resultTable.datagrid('hideColumn', "nextAction");
        </#if>
        refreshHeader();
        Page_ds_dataset.setParameter("dataReviseStatus", "R");
        Page_ds_interface_dataset.setValue('dataReviseStatus', "R");
    });

    function hideAllButton() {
        $("#BTN_ADD").hide();
        $("#BTN_DEL").hide();
        $("#BTN_CHECK").hide();
        $("#btUpload").hide();
        $("#btProblemExport").hide();
        $("#BTN_COMMIT").hide();
        $("#BTN_ROLLBACK").hide();
        $("#BTN_APPROVE").hide();
        $("#BTN_REJECT").hide();
        $("#BTN_SEND_BACK").hide();
    }

    function checkStatusDict(value) {
        if (value === 'Y') {
            return '校验通过';
        } else if (value === 'N') {
            return '未校验';
        } else if (value === 'M') {
            return '校验中';
        } else if (value === 'F') {
            return '校验未通过';
        } else {
            return '';
        }
    }

    function nextActionDict(value) {
        if (value === '00') {
            return '待补录';
        } else if (value === '10') {
            return '待复核';
        } else if (value === '20') {
            return '待审核';
        } else if (value === '30') {
            return '审核通过';
        } else if (value === '99') {
            return '等待';
        } else {
            return '';
        }
    }


    <#-- 所有dataset刷新完数据后触发，调用此方法 -->

    function initCallGetter_post() {
        if (checkFlag === "F") {
            $("#Imas" + report_code + "_ds_interface_dataset_btnSubmit").click()
        }
        Page_ds_interface_dataset.setValue('sjrq', dataRptDate);
        Page_ds_interface_dataset.setFieldReadOnly('sjrq', true);
        // Page_ds_interface_dataset.setValue('dataReviseStatus', "R");
        if (pageType == "M") {
            setInterfaceFieldHidden("orgId");
            // setInterfaceFieldHidden("groupId");
        }
        if (pageType == "B") {
            Page_ds_interface_dataset.setFieldRequired("groupId", true);
            Page_ds_interface_dataset.setValue('groupId', groupId);
            document.getElementById("BTN_ADD").style.display = 'none';
            document.getElementById("BTN_MOD").style.display = 'none';
            opr = "qry";
        }
    }

    function setInterfaceFieldHidden(fieldName) {
        var field = null;
        field = document.getElementById("select_" + fieldName);
        if (null != field) {
            field.parentNode.style.display = 'none';
        }

        field = document.getElementById("fldlabel_" + fieldName);
        if (null != field) {
            field.parentNode.style.display = 'none';
        }

        field = document.getElementById("editor_" + fieldName);
        if (null != field) {
            field.parentNode.style.display = 'none';
        }
    }

    function resultTb_opr_onRefresh(cell, value, record) {
        if (record) {//当存在记录时
            var dataId = record.getValue("dataId");
            var nextAction = record.getValue("nextAction");
            if (("00" === nextAction || "99" === nextAction) && pageType !== "F" && pageStatus === "00" && pageType !== "R" && hasModPrivilege) {
                cell.innerHTML = "<a href=\"JavaScript:qryDetail('" + dataId + "')\">查看</a>&nbsp;&nbsp;<a href=\"JavaScript:modDetail('" + dataId + "')\">修改</a>";
            } else {
                cell.innerHTML = "<a href=\"JavaScript:qryDetail('" + dataId + "')\">查看</a>";
            }

        } else {//当不存在记录时
            cell.innerHTML = "&nbsp;";
        }
    }

    /**
     * @return {boolean}
     */
    function BTN_ADD_onClickCheck(button) {
        var records = getSelectedRecord();
        var dataId = "no_data_id";
        var opr_desc = "新增";
        if (records.length === 1) {
            dataId = getRecord().getValue("dataId");
            opr_desc = "克隆"
        } else if (records.length > 1) {
            easyMsg.warn("只能选择一笔数据进行克隆！");
            return false;
        }
        openDetail("${recordOpr.ADD}", opr_desc, dataId);
        return true;
    }

    function qryDetail(dataId) {
        openDetail("qry", "查看", dataId);
    }

    function modDetail(dataId) {
        openDetail("${recordOpr.MOD}", "修改", dataId);
    }

    function openDetail(opr, title, dataId) {
        var url = Page_path + "&mode=${meta.extra.oprMode}&opr=" + opr + "&dataId=" + dataId + "&targetGroupId=" +
            targetGroupId + "&taskOrgId=" + targetOrgId + "&dataRptDate=" + dataRptDate + "&taskId=" + taskId +
            "&showChangedFieldColor=N&defaultDataReviseStatus=R" + "&changeViewMode=" + changeViewMode +
            "&reportCode=" + report_code + "&hisQuery=" + pageType;
        showWin(title, url, "window", "", window);
    }

    /**
     * @return {boolean}
     */
    function BTN_DEL_onClickCheck(button, handleClick) {
        set_dataset_parameter("${recordOpr.DEL}", "${batchMode.SELECTED}", "N", "N");
        queryOprs("删除", "${nextActionEnum.TO_REVISE.getCode()}", "待补录", handleClick);
        return false;
    }

    /**
     * @return {boolean}
     */
    function BTN_DEL_postSubmit(button) {
        return common_postSubmit(button);
    }

    /**
     * @return {boolean}
     */
    function BTN_CHECK_onClickCheck(button, handleClick) {
        set_dataset_parameter("${recordOpr.CHECK}", "${batchMode.SELECTED}", "N", "N");
        queryOprs("校验", "${nextActionEnum.TO_REVISE.getCode()}", "待补录", handleClick);
        return false;
    }

    /**
     * @return {boolean}
     */
    function BTN_CHECK_postSubmit(button) {
        var retParam = button.returnParam; //获取后台返回参数
        var code = retParam.code;
        if ("" === code || "00" === code || code === null) {
            top.easyMsg.info("操作成功!");
        } else {
            top.easyMsg.warn(code + retParam.desc);
        }

        Page_ds_dataset.flushData(1);
        refreshHeader();
        return true;
    }

    /**
     * @return {boolean}
     */
    function BTN_APPROVE_onClickCheck(button, handleClick) {
        if (Page_ds_dataset.getParameter("force_commit_confirm") === "Y") {
            // 已经审核过 检测到有校验未通过的数据 用户确认强制提交后重新触发点击按钮后进入此逻辑
            var totalCount = Page_ds_dataset.totalCount;
            set_dataset_parameter("${recordOpr.APPROVE}", "${batchMode.QUERY}", "Y", "Y");
            Page_ds_dataset.setParameter("fetchedCount", totalCount);
            // 参数重置
            Page_ds_dataset.setParameter("force_commit_confirm", "N");
            var record = Page_ds_dataset.getFirstRecord();
            while (record) {
                record.setValue("select", true);
                record = record.getNextRecord();
            }
            return true;
        } else {
            set_dataset_parameter("${recordOpr.APPROVE}", "${batchMode.SELECTED}", "N", "Y");
            queryOprs("审核通过", "${nextActionEnum.TO_APPROVE.getCode()}", "待审核", handleClick);
        }
        return false;
    }

    /**
     * @return {boolean}
     */
    function BTN_APPROVE_postSubmit(button) {
        var retParam = button.returnParam; //获取后台返回参数
        var code = retParam.code;
        var desc = retParam.desc;
        if (code === "confirm") {
            top.easyMsg.confirm(desc, function () {
                Page_ds_dataset.setParameter("isForce", "Y");
                Page_ds_dataset.setParameter("force_commit_confirm", "Y");
                $("#BTN_APPROVE").click();
            }, function () {
                return false;
            }, {btnReverse: true});
        } else {
            return common_postSubmit(button);
        }
    }

    /**
     * @return {boolean}
     */
    function BTN_COMMIT_onClickCheck(button, handleClick) {
        if (Page_ds_dataset.getParameter("force_commit_confirm") === "Y") {
            // 已经提交过 检测到有校验失败记录 用户确认强制提交后重新触发点击按钮后进入此逻辑
            var totalCount = Page_ds_dataset.totalCount;
            set_dataset_parameter("${recordOpr.COMMIT}", "${batchMode.QUERY}", "Y", "Y");
            Page_ds_dataset.setParameter("fetchedCount", totalCount);
            // 参数重置
            Page_ds_dataset.setParameter("force_commit_confirm", "N");
            var record = Page_ds_dataset.getFirstRecord();
            while (record) {
                record.setValue("select", true);
                record = record.getNextRecord();
            }
            return true;
        } else {
            set_dataset_parameter("${recordOpr.COMMIT}", "${batchMode.SELECTED}", "N", "Y");
            queryOprs("提交", "${nextActionEnum.TO_REVISE.getCode()}", "待补录", handleClick);
        }
        return false;
    }

    /**
     * @return {boolean}
     */
    function BTN_COMMIT_postSubmit(button) {
        var retParam = button.returnParam; //获取后台返回参数
        var code = retParam.code;
        var desc = retParam.desc;
        if (code === "confirm") {
            top.easyMsg.confirm(desc, function () {
                Page_ds_dataset.setParameter("isForce", "Y");
                Page_ds_dataset.setParameter("force_commit_confirm", "Y");
                $("#BTN_COMMIT").click();
            }, function () {
                return false;
            }, {btnReverse: true});
        } else {
            return common_postSubmit(button);
        }
    }

    /**
     * @return {boolean}
     */
    function BTN_ROLLBACK_onClickCheck(button, handleClick) {
        set_dataset_parameter("${recordOpr.ROLLBACK}", "${batchMode.SELECTED}", "N", "Y");
        queryOprs("撤回", "${nextActionEnum.TO_APPROVE.getCode()}", "待审核", handleClick);
        return false;
    }

    /**
     * @return {boolean}
     */
    function BTN_ROLLBACK_postSubmit(button) {
        return common_postSubmit(button);
    }

    /**
     * @return {boolean}
     */
    function BTN_REJECT_onClickCheck(button, handleClick) {
        set_dataset_parameter("${recordOpr.REJECT}", "${batchMode.SELECTED}", "N", "Y");
        queryOprs("拒绝", "${nextActionEnum.TO_APPROVE.getCode()}", "待审核", handleClick);
        return false;
    }

    /**
     * @return {boolean}
     */
    function BTN_REJECT_postSubmit(button) {
        return common_postSubmit(button);
    }

    /**
     * @return {boolean}
     */
    function BTN_SEND_BACK_onClickCheck(button, handleClick) {
        set_dataset_parameter("${recordOpr.SEND_BACK}", "${batchMode.SELECTED}", "N", "Y");
        queryOprs("打回", "${nextActionEnum.APPROVED.getCode()}", "已审核", handleClick);
        return false;
    }

    function set_dataset_parameter(opr, batchMode, isForce, isCommit) {
        Page_ds_dataset.setParameter("targetGroupId", targetGroupId);
        Page_ds_dataset.setParameter("targetOrgId", targetOrgId);
        Page_ds_dataset.setParameter("opr", opr);
        Page_ds_dataset.setParameter("batch", batchMode);
        Page_ds_dataset.setParameter("isForce", isForce);
        Page_ds_dataset.setParameter("isCommit", isCommit);
    }

    /**
     * @return {boolean}
     */
    function BTN_SEND_BACK_postSubmit(button) {
        return common_postSubmit(button);
    }

    /**
     * @return {boolean}
     */
    function common_postSubmit(button) {
        var retParam = button.returnParam; //获取后台返回参数
        var code = retParam.code;
        if ("" === code || "00" === code || code === null) {
            top.easyMsg.info("操作成功!");
        } else {
            top.easyMsg.warn(retParam.desc, {timeout: 10 * 1000});
        }

        Page_ds_dataset.flushData(1);
        refreshHeader();
        return true;
    }

    /**
     * @return {boolean}
     */
    var productMode = "${productMode}";

    function BTN_CHECK_RESULT_onClickCheck(button, handleClick) {
        var dataIdsArray = [];
        var records = getSelectedRecord();
        for (i = 0; i < records.length; i++) {
            dataIdsArray.push(records[i].getValue("dataId"));
        }
        var dataIds;
        if (dataIdsArray.length > 0) {
            dataIds = dataIdsArray.join(",");
        }
        var querStr = [];
        if (records.length == 0) {
            var whereFields = Page_ds_interface_dataset.fields;
            let whereJ = 0, whereLen = whereFields.length;
            // 是否有查询条件
            for (; whereJ < whereLen; whereJ++) {
                var whereField = whereFields[whereJ];
                if (whereField.name.startWith("_cur_") || whereField.name.startWith("_old_")) {
                    continue;
                }
                if (whereField.dropDown !== "" && whereField.name.endWith("name")) {
                    continue;
                }
                if (whereField.dataType === "date") {
                    var whereValue = Page_ds_interface_dataset.getString(whereField.name);
                } else {
                    var whereValue = Page_ds_interface_dataset.getValue(whereField.name);
                }
                if (whereValue != null && whereValue !== "") {
                    if($.inArray(whereField.fieldName, ["orgId", "groupId", "dataOp", "dataFlag", "dataRptFlag"]) >= 0){
                        querStr.push(whereField.fieldName + "=" + whereValue.replace(/,/g, "____"));
                    }else{
                        querStr.push(whereField.fieldName + "=" + whereValue);
                    }
                }
            }
            // 参数
            var parameters = Page_ds_interface_dataset.parameters;
            for (let i = 0; i < parameters.length; i++) {
                var parameter = parameters[i];
                if (parameter.value != null && parameter.value !== "") {
                    if (parameter.name === "_listParameters") {
                        querStr.push(parameter.name + "=" + parameter.value.replace(/,/g, "____"));
                    } else {
                        querStr.push(parameter.name + "=" + parameter.value);
                    }
                }
            }
        }
        var showBtn = "Y";
        if (productMode === "BIZ") {
            showBtn = "N";
        }

        showWin("校验失败明细", "/pages/imas/rpt/commonjs/check/CheckErrorsDetail.ftl?dataIds=" + dataIds + "&dataRptDate=" +
            dataRptDate + "&reportCode=" + report_code + "&groupId=" + targetGroupId + "&query=" + querStr.join(",")
            + "&orgId=" + targetOrgId + "&reportName=" + encodeURIComponent(report_name) + "&showBtn=" + showBtn +
            "&tDataId=undefine&pageStatus=" + pageStatus, "window",
            "",
            window);
    }

    function queryOprs(oprDesc, expectStatus, expectStatusDesc, handleClick) {
        var whereFields = Page_ds_interface_dataset.fields;
        let whereJ = 0, whereLen = whereFields.length;
        // 是否有查询条件
        var hasCond = false;
        for (; whereJ < whereLen; whereJ++) {
            var whereField = whereFields[whereJ];
            if (whereField.name.startWith("_cur_") || whereField.name.startWith("_old_")) {
                continue;
            }
            if ($.inArray(whereField.fieldName, ["sjrq", "dataReviseStatus", "nextAction"]) >= 0) {
                continue;
            }
            if (whereField.dropDown !== "" && whereField.name.endWith("name")) {
                continue;
            }
            var whereValue = Page_ds_interface_dataset.getValue(whereField.name);
            if (whereValue != null && whereValue !== "") {
                hasCond = true;
                break;
            }
        }
        var totalCount = Page_ds_dataset.totalCount;
        if (hasCond && totalCount === 0) {
            // 有查询条件 且 数据总数为0
            top.easyMsg.error("无可" + oprDesc + "数据！");
            return false;
        } else if (!hasCond && totalCount === 0) {
            // 无查询条件 且 数据总数为0
            datasetQueryCallback = function () {
                totalCount = Page_ds_dataset.totalCount;
                if (totalCount === 0) {
                    // 点击查询后 查询结果仍然没有数据 则 确认为空报表
                    top.easyMsg.confirm("当前空数据，确认是否" + oprDesc + "？", function () {
                        Page_ds_dataset.setParameter("isEmpty", "Y");
                        Page_ds_dataset.insertRecord();
                        var record = Page_ds_dataset.getFirstRecord();
                        record.setValue("select", true);
                        handleClick();
                        Page_ds_dataset.clearData();
                    });
                } else {
                    // 点击查询后 查询结果有数据 则为查询提交等操作，提示用户输入对应的查询条件
                    return doQueryOpr(oprDesc, expectStatus, expectStatusDesc, handleClick);
                }
            };
            // 点击查询
            $("#" + query_submit_btn_name).click();
        } else {
            var selectedRecords = getSelectedRecord();
            if (selectedRecords.length === 0) {
                // 用户未选择数据 则认为是查询提交等操作
                return doQueryOpr(oprDesc, expectStatus, expectStatusDesc, handleClick);
            } else {
                // 用户选择了数据 则认为是选择提交等操作
                let hasNotCheckSuccess = false;
                let hasCheckFailedSuccess = false;
                let j = 0, len = selectedRecords.length;
                for (; j < len; j++) {
                    const record = selectedRecords[j];
                    if (record.getValue("nextAction") !== expectStatus) {
                        top.easyMsg.warn("所选数据中包含不为“" + expectStatusDesc + "”的数据！");
                        return false;
                    }
                    if (record.getValue("dataOp") !== "${opFlag.DATA_DEL.getCode()}") {
                        if (record.getValue("checkFlag") === "F") {
                            hasCheckFailedSuccess = true;
                        }
                        if (record.getValue("checkFlag") === "M" || record.getValue("checkFlag") === "N") {
                            hasNotCheckSuccess = true;
                        }

                    }
                }
                // 提交特殊化处理
                if (oprDesc === "提交" && hasNotCheckSuccess && $.inArray(report_code, ["A1411", "A2411"]) < 0) {
                    top.easyMsg.error("包含“校验中”或“未校验”的数据，不能提交！")
                } else if ((oprDesc === "提交" || oprDesc === "审核通过") && hasCheckFailedSuccess && $.inArray(report_code, ["A1411", "A2411"]) < 0) {
                    top.easyMsg.confirm("包含校验未通过数据，是否强制提交？", function () {
                        Page_ds_dataset.setParameter("isForce", "Y");
                        top.easyMsg.confirm("确认是否" + oprDesc + "选中的" + selectedRecords.length + "条记录？", handleClick);
                    });
                } else {
                    top.easyMsg.confirm("确认是否" + oprDesc + "选中的" + selectedRecords.length + "条记录？", handleClick);
                }
            }
        }

    }

    function doQueryOpr(oprDesc, expectStatus, expectStatusDesc, handleClick) {
        console.log("do query opr " + oprDesc)
        let nextAction = Page_ds_interface_dataset.getValue("nextAction");
        if (nextAction == null || nextAction === "") {
            Page_ds_interface_dataset.setValue("nextAction", expectStatus);
        } else if (nextAction !== expectStatus) {
            top.easyMsg.warn("查询" + oprDesc + "时，数据状态查询条件必须为“" + expectStatusDesc + "”！");
            return false;
        }

        datasetQueryCallback = function () {
            let totalCount = Page_ds_dataset.totalCount;
            if (totalCount === 0) {
                top.easyMsg.warn("无可" + oprDesc + "数据！");
                return;
            }
            top.easyMsg.confirm("确认是否" + oprDesc + "查询到的" + totalCount + "条记录？", function () {
                Page_ds_dataset.setParameter("batch", "${batchMode.QUERY}");
                Page_ds_dataset.setParameter("fetchedCount", totalCount);
                let record = Page_ds_dataset.getFirstRecord();
                while (record) {
                    record.setValue("select", true);
                    record = record.getNextRecord();
                }
                handleClick();
            }, function () {
                return false;
            }, {btnReverse: true});
        };
        $("#" + query_submit_btn_name).click();
    }


    function getSelectedRecord() {
        var records = [];
        var record = Page_ds_dataset.getFirstRecord();
        while (record) {
            if (record.getValue("select") === true) {
                records.push(record);
            }
            record = record.getNextRecord();
        }
        return records;
    }

    function getRecord() {
        var record = Page_ds_dataset.getFirstRecord();
        while (record) {
            if (record.getValue("select") === true) {
                return record;
            }
            record = record.getNextRecord();
        }
        return null;
    }

    function btExport_onClick() {
        datasetQueryCallback = function () {
            var totalCount = Page_ds_dataset.totalCount;
            if (totalCount === 0) {
                top.easyMsg.warn("无可导出数据！");
                return;
            }
            var url = "/pages/gpms/platform/export/ftl/fileExport.ftl?cqId=" + Page_ds_dataset.cqId;
            showWin("文件导出", url, "window", "", window, 650, 455, "", false);
        };
        $("#" + query_submit_btn_name).click();
    }

    //问题导出功能
    function btProblemExport_onClick() {
        var fileExistsUrl = "${contextPath}/api/imas/exportfile/problemFileExist?dataRptDate=" + dataRptDate + "&reportCode=" + report_code;
        $.get(fileExistsUrl, function (result) {
            result = eval('(' + result + ')');
            if (result.returnMessage === true || result.returnMessage === 'true') {
                window.location.href = "${contextPath}/api/imas/exportfile/problem?guid=" + encodeURI(encodeURI(result.fileName));
            } else {
                alert("文件不存在。");
            }
        });
    }

    function refreshds() {
        Page_ds_dataset.flushData(1);
        refreshHeader();
    }

    function refreshHeader() {
        if (taskId !== "") {
            $.get("${contextPath}/api/imas/record/header/status/" + taskId, function (data) {
                if (data) {
                    $("#task_info_header_right").html("报表:" + report_name + " 操作机构:" + targetOrgId + " 业务线:" + targetGroupId + " 报表日期:" + dataRptDate + " 校验状态:" + checkStatusDict(data.checkFlag) + " 数据状态:" + nextActionDict(data.nextAction));
                }
            }).fail(function () {
                $("#task_info_header_right").html("报表:" + report_name + " 操作机构:" + targetOrgId + " 业务线:" + targetGroupId + " 报表日期:" + dataRptDate);
            });
        } else {
            $("#task_info_header_right").html("报表:" + report_name + " 报表日期:" + dataRptDate);
        }

    }

    <#-- btUpload点击事件 -->

    function btUpload_onClick() {
        // currentSubWin = openSubWin("pageWin", "文件导入", "/pages/rdms/upload/ReviseFileUpload.ftl?type=upload&sysId=" + system_type, "550");
        showWin("文件导入", '/pages/imas/rpt/commonjs/imp/fileImport.ftl?sysId=imas&dataRptDate=' + dataRptDate, "window", "", window, 650, 455, "", false);
    }

    function getFieldInitValue(field) {
        var $field = $('#' + field);
        var _dataset = getDatasetByID($field.attr('componentDataset'));
        var dropdowndataset = copyDataset(
            '_tmp_' + $field.attr('datasetName') + $.uuid++,
            $field.attr('datasetName')
        );
        dropdowndataset.flushData(1);
        var record = dropdowndataset.getFirstRecord();
        return record[0];
    }

    function btReasonDown_onClick() {
        var totalCount = Page_ds_dataset.totalCount;
        if (totalCount === 0) {
            top.easyMsg.warn("无可导出数据！");
            return;
        }
        var url = "/pages/imas/rpt/commonjs/imp/fileExport.ftl?cqId=" + Page_ds_force_submit_exporter_dataset;
        showWin("文件导出", url, "window", "", window, 650, 455, "", false);
    }

    <#-- btUpload点击事件 -->

    function btReasonUp_onClick() {
        showWin("强制提交原因文件导入", '/pages/imas/import/ftl/fileImport.ftl?cqId=' + Page_ds_force_submit_exporter_dataset + '&sysId=imas&dataRptDate=' + dataRptDate,
            "window", "", window, 650, 455, "", false);
    }

</script>
<#include "/pages/imas/rpt/commonjs/Special_Revise_mat.js.ftl"/>