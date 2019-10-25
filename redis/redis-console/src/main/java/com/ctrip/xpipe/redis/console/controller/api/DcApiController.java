package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcModel;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * @author taotaotu
 * May 23, 2019
 */

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class DcApiController extends AbstractConsoleController {

    @Autowired
    private DcService dcService;

    @RequestMapping(value = "/dc", method = RequestMethod.POST)
    public RetMessage addDc(HttpServletRequest request, @RequestBody DcModel dcModel){

        logger.info("[DcApiController][addDc]request ip:{}, create a new dc, zoneId:{}, dc_name:{}, description:{}",request.getRemoteHost(),
                dcModel.getZone_id(), dcModel.getDc_name(), dcModel.getDescription());

        try {
            dcService.insertWithPartField(dcModel.getZone_id(), dcModel.getDc_name(), dcModel.getDescription());
        }catch (Exception e){
            logger.error("[DcApiController][addDc]add dc exception!", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

}
