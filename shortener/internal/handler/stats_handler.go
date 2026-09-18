package handler

import (
	"net/http"

	validation "github.com/go-playground/validator/v10"
	"github.com/zeromicro/go-zero/rest/httpx"
	"shortener/shortener/internal/logic"
	"shortener/shortener/internal/svc"
	"shortener/shortener/internal/types"
)

// StatsHandler 查询指定短链的累计访问总次数
func StatsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.StatsRequest
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		if err := validation.New().StructCtx(r.Context(), &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		l := logic.NewStatsLogic(r.Context(), svcCtx)
		resp, err := l.Stats(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
