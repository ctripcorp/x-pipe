package com.ctrip.xpipe.redis.console.web.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HomeController {
	
	@RequestMapping("/")
	public String xpipeConsoleHome() {
		return "login";
	}
}
