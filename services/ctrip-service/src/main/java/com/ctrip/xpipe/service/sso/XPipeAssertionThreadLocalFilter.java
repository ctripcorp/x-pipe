package com.ctrip.xpipe.service.sso;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 16, 2017
 */
import com.ctrip.xpipe.sso.AbstractFilter;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.jasig.cas.client.util.AssertionHolder;
import org.jasig.cas.client.validation.Assertion;

public class XPipeAssertionThreadLocalFilter extends AbstractFilter implements Filter {


    public XPipeAssertionThreadLocalFilter() {
    }


    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest)servletRequest;
        HttpSession session = request.getSession(false);
        if(!this.needFilter(request)) {
            filterChain.doFilter(request, servletResponse);
        } else {
            Assertion assertion = null;
            if(session != null) {
                assertion = (Assertion)session.getAttribute("_const_cas_assertion_");
            }

            if(assertion == null) {
                assertion = (Assertion)request.getAttribute("_const_cas_assertion_");
            }

            try {
                AssertionHolder.setAssertion(assertion);
                filterChain.doFilter(servletRequest, servletResponse);
            } finally {
                AssertionHolder.clear();
            }

        }
    }

    public void destroy() {
    }
}
