package com.yosep.server.common.component.k8s

/** 풀메시/배포 관련 변화 알림을 받고 싶은 컴포넌트가 구현 */
interface MeshTopologyListener {
    /**
     * 대상 서비스의 Ready 엔드포인트가 바뀌었을 때 호출
     * @param service 서비스 이름(예: finda-mydata-external-server)
     * @param endpoints "host:port" 셋 (Ready만)
     */
    fun onReadyEndpointsChanged(service: String, endpoints: Set<String>) {}

    /**
     * 대상 롤아웃이 Healthy(배포 완료) 전환될 때 호출
     * @param rolloutName 롤아웃 이름
     */
    fun onRolloutHealthy(rolloutName: String) {}
}
