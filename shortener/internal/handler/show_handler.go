package handler

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"
	"shortener/shortener/internal/logic"
	"shortener/shortener/internal/svc"
	"shortener/shortener/internal/types"
)

// 查看短链接的业务逻辑
func ShowHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ShowRequest
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewShowLogic(r.Context(), svcCtx)
		resp, err := l.Show(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			// 返回重定向的http响应 302
			//httpx.OkJsonCtx(r.Context(), w, resp)
			http.Redirect(w, r, resp.LongURL, http.StatusFound)
		}
	}
}
